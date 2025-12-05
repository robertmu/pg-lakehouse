pub mod table_options;
pub mod tablespace_options;

pub fn init_hooks() {
    tablespace_options::init_hook();
    table_options::init_hook();
}
