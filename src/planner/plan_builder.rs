use crate::errors::{MySQLError, MySQLResult};
use crate::planner::{CreateTablePlan, PlanNode, PointGetPlan};
use crate::session::SessionRef;
use crate::table::schema::TableInfo;
use sqlparser::ast::{
    ColumnDef, ObjectName, Query, SqlOption, Statement,
    TableConstraint,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

pub struct PlanBuilder {
    session: SessionRef,
}

impl PlanBuilder {
    pub fn create(session: SessionRef) -> Self {
        Self { session }
    }

    pub fn build_from_sql(&self, query: &str) -> MySQLResult<PlanNode> {
        let dialect = MySqlDialect {};
        let statement = Parser::parse_sql(&dialect, query)?;
        if statement.len() != 1 {
            return Result::Err(MySQLError::UnsupportSQL);
        }
        statement
            .first()
            .map(|s| self.statement_to_plan(&s))
            .unwrap()
    }

    pub fn statement_to_plan(&self, statement: &Statement) -> MySQLResult<PlanNode> {
        match statement {
            Statement::Query(q) => self.sql_query_to_plan(q),
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
        let table_info = TableInfo::create(name, column_defs, constrains)?;
        Ok(PlanNode::CreateTable(CreateTablePlan { table_info }))
    }
}
