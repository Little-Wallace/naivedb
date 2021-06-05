mod create_table_plan;
mod insert_plan;
mod plan_builder;
mod point_get_plan;

pub use create_table_plan::CreateTablePlan;
pub use insert_plan::InsertPlan;
pub use point_get_plan::PointGetPlan;
pub use plan_builder::PlanBuilder;

pub enum PlanNode {
    CreateTable(CreateTablePlan),
    PointGet(PointGetPlan),
    Insert(InsertPlan),
}
