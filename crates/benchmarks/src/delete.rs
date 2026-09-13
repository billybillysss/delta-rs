use std::sync::Arc;

use deltalake_core::datafusion::prelude::SessionContext;
use deltalake_core::delta_datafusion::{create_session, DeltaScanNext};
use deltalake_core::kernel::{Action, Add, DataType, PrimitiveType, StructField, StructType};
use deltalake_core::{arrow, DeltaTable};
use object_store::{path::Path, ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;

pub struct DeleteInput {
    pub table: DeltaTable,
    pub session: SessionContext,
}

pub async fn prepare_delete_input(stats: Option<&str>) -> anyhow::Result<DeleteInput> {
    let delta_schema = StructType::try_new(vec![
        StructField::new(
            "part".to_string(),
            DataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Long),
            true,
        ),
    ])?;
    let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("part", arrow::datatypes::DataType::Utf8, true),
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["a"; 1024])),
            Arc::new(arrow::array::Int64Array::from_iter_values(0..1024)),
        ],
    )?;
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, arrow_schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    let path = "part-00000.parquet";
    let add = Add {
        path: path.to_string(),
        partition_values: [(String::from("part"), Some(String::from("a")))]
            .into_iter()
            .collect(),
        size: bytes.len() as i64,
        modification_time: chrono::Utc::now().timestamp_millis(),
        data_change: true,
        stats: stats.map(str::to_owned),
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    };
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(delta_schema.fields().cloned())
        .with_partition_columns(vec!["part"])
        .with_actions(vec![Action::Add(add)])
        .await?;
    table
        .object_store()
        .put(&Path::from(path), PutPayload::from(bytes))
        .await?;

    let provider = DeltaScanNext::builder()
        .with_log_store(table.log_store())
        .build()
        .await?;
    let session = create_session().into_inner();
    session.register_table("delta_table", Arc::new(provider))?;

    Ok(DeleteInput { table, session })
}

pub async fn run_sql_delete(
    input: &DeleteInput,
) -> anyhow::Result<Vec<arrow::record_batch::RecordBatch>> {
    Ok(input
        .session
        .sql("DELETE FROM delta_table WHERE part = 'a'")
        .await?
        .collect()
        .await?)
}

pub async fn run_direct_delete(
    input: DeleteInput,
) -> anyhow::Result<(
    DeltaTable,
    deltalake_core::operations::delete::DeleteMetrics,
)> {
    Ok(input.table.delete().with_predicate("part = 'a'").await?)
}

#[cfg(test)]
mod tests {
    use deltalake_core::arrow::array::UInt64Array;

    use super::*;

    #[tokio::test]
    async fn delete_operations_report_expected_row_counts() -> anyhow::Result<()> {
        let sql = run_sql_delete(&prepare_delete_input(None).await?).await?;
        let count = sql[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("SQL DELETE count column");
        assert_eq!(count.value(0), 1024);

        let (_, with_stats) =
            run_direct_delete(prepare_delete_input(Some(r#"{"numRecords":1024}"#)).await?).await?;
        assert_eq!(with_stats.num_deleted_rows, Some(1024));

        let (_, without_stats) = run_direct_delete(prepare_delete_input(None).await?).await?;
        assert_eq!(without_stats.num_deleted_rows, None);

        Ok(())
    }
}
