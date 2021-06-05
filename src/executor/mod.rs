

pub mod ddl;

pub trait Executor: Send {
    fn execute(&mut self) -> Mysq
}