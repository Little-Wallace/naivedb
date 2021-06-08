use crate::expression::Expr;

pub struct SelectionPlan {
    pub predicates: Vec<Expr>,
}
