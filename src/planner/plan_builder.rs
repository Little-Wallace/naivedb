use crate::errors::{MySQLError, MySQLResult};
use crate::expression::Expr;
use crate::planner::{CreateTablePlan, PlanNode, PointGetPlan, SelectPlan};
use crate::session::SessionRef;
use crate::table::schema::TableInfo;
use sqlparser::ast;
use sqlparser::ast::{ColumnDef, Ident, ObjectName, Query, SqlOption, Statement, TableConstraint};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

use super::selection_plan::SelectionPlan;

pub struct PlanBuilder {
    session: SessionRef,
}

impl PlanBuilder {
    pub fn create(session: SessionRef) -> Self {
        Self { session }
    }

    pub fn build_from_sql(&self, query: &str) -> MySQLResult<PlanNode> {
        let dialect = MySqlDialect {};
        println!("build_from_sql");
        let statement = Parser::parse_sql(&dialect, query)?;
        println!("build_from_sql get {}", statement.len());
        if statement.len() != 1 {
            return Result::Err(MySQLError::UnsupportSQL);
        }
        statement
            .first()
            .map(|s| self.statement_to_plan(&s))
            .unwrap()
    }

    pub fn statement_to_plan(&self, statement: &Statement) -> MySQLResult<PlanNode> {
        println!("{:?}", statement);
        match statement {
            Statement::Query(q) => self.sql_query_to_plan(q),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => self.sql_insert_to_plan(table_name, columns, source),
            Statement::CreateTable {
                name,
                columns,
                constraints,
                without_rowid,
                or_replace,
                table_properties,
                ..
            } => self.sql_create_table_to_plan(
                name,
                columns,
                constraints,
                *without_rowid,
                *or_replace,
                table_properties,
            ),
            _ => return Err(MySQLError::UnsupportSQL),
        }
    }

    fn sql_query_to_plan(&self, query: &Box<Query>) -> MySQLResult<PlanNode> {
        let point_get_builder = QueryPlanBuilder::new(self.session.clone());
        if let Some(plan) = point_get_builder.try_point_get(query)? {
            return Ok(PlanNode::PointGet(plan));
        }
        unimplemented!()
    }

    fn sql_create_table_to_plan(
        &self,
        name: &ObjectName,
        column_defs: &Vec<ColumnDef>,
        constrains: &Vec<TableConstraint>,
        _without_rowid: bool,
        _or_replace: bool,
        _table_properties: &Vec<SqlOption>,
    ) -> MySQLResult<PlanNode> {
        let table_info = Arc::new(TableInfo::create(name, column_defs, constrains)?);
        Ok(PlanNode::CreateTable(CreateTablePlan { table_info }))
    }

    fn sql_selection_to_plan(&self, selection: &ast::Expr) -> MySQLResult<PlanNode> {
        let predicate = self.sql_expression_to_expr(selection)?;
        Ok(PlanNode::Selection(SelectionPlan {
            predicates: vec![predicate],
        }))
    }

    fn sql_expression_to_expr(&self, expr: &ast::Expr) -> MySQLResult<Expr> {
        match expr {
            ast::Expr::BinaryOp { left, op, right } => match op {
                ast::BinaryOperator::Eq => Ok(Expr::Equal {
                    left: Box::new(self.sql_expression_to_expr(&left)?),
                    right: Box::new(self.sql_expression_to_expr(&right)?),
                }),
                _ => Err(MySQLError::UnsupportSQL),
            },
            _ => Err(MySQLError::UnsupportSQL),
        }
    }

    fn sql_insert_to_plan(
        &self,
        table_name: &ObjectName,
        columns: &[Ident],
        source: &Query,
    ) -> MySQLResult<PlanNode> {
        Ok(PlanNode::Select(SelectPlan{}))
    }
}
