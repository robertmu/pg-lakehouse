pub mod tablespace;

pub fn init_hooks() {
    tablespace::init_hook();
}
