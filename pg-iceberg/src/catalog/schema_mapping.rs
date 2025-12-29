//! PostgreSQL to Iceberg type conversion.
//!
//! This module provides functions to convert PostgreSQL tuple descriptors
//! to Iceberg schemas.

use crate::error::{IcebergError, IcebergResult};
use iceberg_lite::spec::{NestedField, PrimitiveType, Schema, Type};
use pgrx::{pg_sys, PgBuiltInOids, PgOid};
use std::ffi::CStr;
use std::sync::Arc;

/// Convert a PostgreSQL type OID to an Iceberg Type.
///
/// # Arguments
/// * `type_oid` - The PostgreSQL type OID
/// * `type_mod` - The type modifier (for types like numeric with precision/scale)
///
/// # Returns
/// The corresponding Iceberg Type, or an error if the type is not supported.
pub fn pg_type_to_iceberg_type(type_oid: pg_sys::Oid, type_mod: i32) -> IcebergResult<Type> {
    let pg_oid = PgOid::from(type_oid);

    match pg_oid {
        // Boolean
        PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => Ok(Type::Primitive(PrimitiveType::Boolean)),

        // Integer types
        PgOid::BuiltIn(PgBuiltInOids::INT2OID) => Ok(Type::Primitive(PrimitiveType::Int)),
        PgOid::BuiltIn(PgBuiltInOids::INT4OID) => Ok(Type::Primitive(PrimitiveType::Int)),
        PgOid::BuiltIn(PgBuiltInOids::INT8OID) => Ok(Type::Primitive(PrimitiveType::Long)),

        // Floating point types
        PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => Ok(Type::Primitive(PrimitiveType::Float)),
        PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => Ok(Type::Primitive(PrimitiveType::Double)),

        // Numeric/Decimal - extract precision and scale from type_mod
        PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
            // type_mod for numeric: ((precision << 16) | scale) + VARHDRSZ
            // VARHDRSZ = 4, so we need to subtract 4 first
            if type_mod >= 0 {
                let adjusted = (type_mod - 4) as u32;
                let precision = (adjusted >> 16) & 0xFFFF;
                let scale = adjusted & 0xFFFF;
                Ok(Type::Primitive(PrimitiveType::Decimal { precision, scale }))
            } else {
                // No precision/scale specified, use defaults
                Ok(Type::Primitive(PrimitiveType::Decimal {
                    precision: 38,
                    scale: 18,
                }))
            }
        }

        // String types
        PgOid::BuiltIn(PgBuiltInOids::TEXTOID)
        | PgOid::BuiltIn(PgBuiltInOids::VARCHAROID)
        | PgOid::BuiltIn(PgBuiltInOids::BPCHAROID)
        | PgOid::BuiltIn(PgBuiltInOids::NAMEOID) => Ok(Type::Primitive(PrimitiveType::String)),

        // Date and Time types
        PgOid::BuiltIn(PgBuiltInOids::DATEOID) => Ok(Type::Primitive(PrimitiveType::Date)),
        PgOid::BuiltIn(PgBuiltInOids::TIMEOID) => Ok(Type::Primitive(PrimitiveType::Time)),
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
            Ok(Type::Primitive(PrimitiveType::Timestamp))
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
            Ok(Type::Primitive(PrimitiveType::Timestamptz))
        }

        // Binary types
        PgOid::BuiltIn(PgBuiltInOids::BYTEAOID) => Ok(Type::Primitive(PrimitiveType::Binary)),

        // UUID
        PgOid::BuiltIn(PgBuiltInOids::UUIDOID) => Ok(Type::Primitive(PrimitiveType::Uuid)),

        // JSON types - store as string
        PgOid::BuiltIn(PgBuiltInOids::JSONOID) | PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => {
            Ok(Type::Primitive(PrimitiveType::String))
        }

        _ => Err(IcebergError::UnsupportedColumnType(format!(
            "OID {}",
            u32::from(type_oid)
        ))),
    }
}

/// Convert a PostgreSQL TupleDesc to an Iceberg Schema.
///
/// # Safety
/// The caller must ensure that `tup_desc` is a valid pointer to a TupleDesc.
///
/// # Arguments
/// * `tup_desc` - A pointer to the PostgreSQL TupleDesc
///
/// # Returns
/// An Iceberg Schema with fields corresponding to the TupleDesc attributes.
pub unsafe fn tuple_desc_to_schema(tup_desc: pg_sys::TupleDesc) -> IcebergResult<Schema> {
    unsafe {
        let natts = (*tup_desc).natts as usize;
        let attrs = std::slice::from_raw_parts((*tup_desc).attrs.as_ptr(), natts);

        let mut fields = Vec::with_capacity(natts);

        for (i, attr) in attrs.iter().enumerate() {
            // Skip dropped columns
            if attr.attisdropped {
                continue;
            }

            // Get column name
            let name_ptr = attr.attname.data.as_ptr();
            let name = CStr::from_ptr(name_ptr).to_string_lossy().to_string();

            // Get Iceberg type from PostgreSQL type
            let iceberg_type = pg_type_to_iceberg_type(attr.atttypid, attr.atttypmod)?;

            // Field ID is 1-based index (Iceberg requires unique positive field IDs)
            let field_id = (i + 1) as i32;

            // Create NestedField based on nullability
            let field = if attr.attnotnull {
                NestedField::required(field_id, name, iceberg_type)
            } else {
                NestedField::optional(field_id, name, iceberg_type)
            };

            fields.push(Arc::new(field));
        }

        Schema::builder()
            .with_fields(fields)
            .build()
            .map_err(|e| IcebergError::SchemaBuildError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_type_to_iceberg_type_int() {
        let result =
            pg_type_to_iceberg_type(pg_sys::Oid::from(PgBuiltInOids::INT4OID.value()), -1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Type::Primitive(PrimitiveType::Int));
    }

    #[test]
    fn test_pg_type_to_iceberg_type_text() {
        let result =
            pg_type_to_iceberg_type(pg_sys::Oid::from(PgBuiltInOids::TEXTOID.value()), -1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Type::Primitive(PrimitiveType::String));
    }

    #[test]
    fn test_pg_type_to_iceberg_type_numeric_with_precision() {
        // numeric(10, 2): type_mod = ((10 << 16) | 2) + 4 = 655366
        let type_mod = ((10i32 << 16) | 2) + 4;
        let result = pg_type_to_iceberg_type(
            pg_sys::Oid::from(PgBuiltInOids::NUMERICOID.value()),
            type_mod,
        );
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Type::Primitive(PrimitiveType::Decimal {
                precision: 10,
                scale: 2
            })
        );
    }
}
