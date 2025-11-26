use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;
use pg_tam::prelude::CreateRuntimeError;

#[derive(Error, Debug)]
pub enum IcebergError {
    #[error("column {0} is not found in source")]
    ColumnNotFound(String),

    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("column '{0}' data type '{1}' is incompatible")]
    IncompatibleColumnType(String, String),

    #[error("cannot import column '{0}' data type '{1}'")]
    ImportColumnError(String, String),

    #[error("decimal conversion error: {0}")]
    DecimalConversionError(#[from] rust_decimal::Error),

    #[error("parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),

    #[error("datetime conversion error: {0}")]
    DatetimeConversionError(#[from] pgrx::datum::datetime_support::DateTimeConversionError),

    #[error("datum conversion error: {0}")]
    DatumConversionError(String),

    #[error("uuid error: {0}")]
    UuidConversionError(#[from] uuid::Error),

    #[error("numeric error: {0}")]
    NumericError(#[from] pgrx::datum::numeric_support::error::Error),

    #[error("iceberg error: {0}")]
    IcebergError(#[from] iceberg::Error),

    #[error("arrow error: {0}")]
    ArrowError(#[from] arrow_schema::ArrowError),

    #[error("json error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("{0}")]
    CreateRuntimeError(#[from] CreateRuntimeError),

    #[error("{0}")]
    IoError(#[from] std::io::Error),

    #[error("feature not yet implemented: {0}")]
    NotImplemented(&'static str),
}

impl From<IcebergError> for ErrorReport {
    fn from(value: IcebergError) -> Self {
        let error_code = match &value {
            IcebergError::ColumnNotFound(_) => PgSqlErrorCode::ERRCODE_UNDEFINED_COLUMN,

            IcebergError::UnsupportedColumnType(_)
            | IcebergError::IncompatibleColumnType(_, _)
            | IcebergError::ImportColumnError(_, _) => PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH,

            IcebergError::DecimalConversionError(_)
            | IcebergError::ParseFloatError(_)
            | IcebergError::DatetimeConversionError(_)
            | IcebergError::DatumConversionError(_)
            | IcebergError::UuidConversionError(_)
            | IcebergError::NumericError(_) => PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,

            IcebergError::IcebergError(_)
            | IcebergError::ArrowError(_)
            | IcebergError::JsonError(_) => PgSqlErrorCode::ERRCODE_FDW_ERROR,

            IcebergError::CreateRuntimeError(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,

            IcebergError::IoError(_) => PgSqlErrorCode::ERRCODE_IO_ERROR,

            IcebergError::NotImplemented(_) => PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
        };
        ErrorReport::new(error_code, format!("{value}"), "")
    }
}

pub type IcebergResult<T> = Result<T, IcebergError>;
