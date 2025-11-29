pub mod tablespace;

pub unsafe fn init_hooks() {
    tablespace::init_hook();
}
