mod create_table_plan;
mod insert_plan;
mod plan_builder;
mod point_get_plan;
mod selection_plan;

pub use create_table_plan::CreateTablePlan;
pub use insert_plan::InsertPlan;
pub use plan_builder::PlanBuilder;
pub use point_get_plan::PointGetPlan;
pub use selection_plan::SelectionPlan;

pub enum PlanNode {
    CreateTable(CreateTablePlan),
    PointGet(PointGetPlan),
    Insert(InsertPlan),
    Selection(SelectionPlan),
}
