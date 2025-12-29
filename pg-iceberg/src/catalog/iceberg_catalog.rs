//! PostgreSQL-based Iceberg Catalog implementation
//!
//! This module provides a PostgreSQL-backed catalog implementation that stores
//! Iceberg table metadata in PostgreSQL system catalogs.

use std::collections::HashMap;
use std::fmt::Debug;

use iceberg_lite::catalog::{
    Catalog, MetadataLocation, Namespace, NamespaceIdent, TableCommit, TableCreation,
    TableIdent,
};
use iceberg_lite::io::FileIO;
use iceberg_lite::spec::TableMetadataBuilder;
use iceberg_lite::table::Table;
use iceberg_lite::{Error, ErrorKind, Result};

/// PostgreSQL-based Iceberg Catalog implementation.
///
/// This catalog stores Iceberg table metadata in PostgreSQL system catalogs,
/// allowing seamless integration between PostgreSQL and Iceberg tables.
#[derive(Debug, Clone)]
pub struct IcebergCatalog {
    /// The name of this catalog
    name: String,
    /// File IO
    file_io: FileIO,
}

impl Default for IcebergCatalog {
    fn default() -> Self {
        Self {
            name: Default::default(),
            file_io: FileIO::memory(),
        }
    }
}

impl IcebergCatalog {
    pub fn new(
        name: impl Into<String>,
        file_io: FileIO,
    ) -> Self {
        Self {
            name: name.into(),
            file_io,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn file_io(&self) -> FileIO {
        self.file_io.clone()
    }
}

/// Checks if provided `NamespaceIdent` is valid.
pub(crate) fn validate_namespace(namespace: &NamespaceIdent) -> Result<String> {
    let name = namespace.as_ref();

    if name.len() != 1 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid database name: {namespace:?}, hierarchical namespaces are not supported"
            ),
        ));
    }

    let name = name[0].clone();

    if name.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Invalid database, provided namespace is empty.",
        ));
    }

    Ok(name)
}

impl Catalog for IcebergCatalog {
    fn list_namespaces<'a>(
        &self,
        _parent: Option<&'a NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        todo!("list_namespaces not yet implemented")
    }

    fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        todo!("create_namespace not yet implemented")
    }

    fn get_namespace(&self, _namespace: &NamespaceIdent) -> Result<Namespace> {
        todo!("get_namespace not yet implemented")
    }

    fn namespace_exists(&self, _namespace: &NamespaceIdent) -> Result<bool> {
        todo!("namespace_exists not yet implemented")
    }

    fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        todo!("update_namespace not yet implemented")
    }

    fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
        todo!("drop_namespace not yet implemented")
    }

    fn list_tables(&self, _namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        todo!("list_tables not yet implemented")
    }

    fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        let db_name = validate_namespace(namespace)?;
        let table_name = creation.name.clone();

        // Location is required - the catalog only manages metadata,
        // storage location must be explicitly specified by the user
        let location = creation.location.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Table location is required. Please specify a storage location for the table.",
            )
        })?;

        let metadata = TableMetadataBuilder::from_table_creation(creation)?
            .build()?
            .metadata;

        let metadata_location =
            MetadataLocation::new_with_table_location(location.clone()).to_string();

        metadata.write_to(&self.file_io, &metadata_location)?;

        // Build and return the Table object
        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(TableIdent::new(NamespaceIdent::new(db_name), table_name))
            .build()
    }

    fn load_table(&self, _table: &TableIdent) -> Result<Table> {
        todo!("load_table not yet implemented")
    }

    fn drop_table(&self, _table: &TableIdent) -> Result<()> {
        todo!("drop_table not yet implemented")
    }

    fn table_exists(&self, _table: &TableIdent) -> Result<bool> {
        todo!("table_exists not yet implemented")
    }

    fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        todo!("rename_table not yet implemented")
    }

    fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> Result<Table> {
        todo!("register_table not yet implemented")
    }

    fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        todo!("update_table not yet implemented")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iceberg_catalog_new() {
        let catalog =
            IcebergCatalog::new("test_catalog", FileIO::memory());

        assert_eq!(catalog.name(), "test_catalog");
    }

    #[test]
    fn test_iceberg_catalog_with_properties() {
        let mut props = HashMap::new();
        props.insert("key1".to_string(), "value1".to_string());
        props.insert("key2".to_string(), "value2".to_string());

        let catalog =
            IcebergCatalog::new("test_catalog", FileIO::memory());

        assert_eq!(catalog.name(), "test_catalog");
    }
}
