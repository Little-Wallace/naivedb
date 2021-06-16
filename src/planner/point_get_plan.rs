use crate::common::EncodeValue;
use crate::errors::{MySQLError, MySQLResult};
use crate::planner::plan_expression::Expression;
use crate::session::{Session, SessionRef};
use crate::table::schema::{ColumnInfo, DataSchema, DataSchemaRef, IndexInfo};
use crate::table::table::TableSource;
use sqlparser::ast::{BinaryOperator, Expr, Query, Select, SelectItem, SetExpr, TableFactor};
use std::sync::Arc;

pub struct PointGetPlan {
    pub table: Arc<TableSource>,
    pub index_info: Arc<IndexInfo>,
    pub index_value: EncodeValue,
    pub select_columns: DataSchemaRef,
    pub filters: Vec<Expression>,
    pub session: SessionRef,
}

pub struct QueryPlanBuilder {
    table: Option<Arc<TableSource>>,
    index_info: Vec<Arc<IndexInfo>>,
    select_columns: Vec<Arc<ColumnInfo>>,
    index_values: Vec<EncodeValue>,
    point_get: bool,
    filters: Vec<Expression>,
    session: SessionRef,
}

impl QueryPlanBuilder {
    pub fn new(session: SessionRef) -> QueryPlanBuilder {
        QueryPlanBuilder {
            table: None,
            index_info: vec![],
            select_columns: vec![],
            index_values: vec![],
            point_get: true,
            filters: vec![],
            session,
        }
    }

    pub fn try_point_get(mut self, query: &Query) -> MySQLResult<Option<PointGetPlan>> {
        self.visit(query)?;
        if self.point_get {
            Ok(Some(PointGetPlan {
                table: self.table.take().unwrap(),
                index_info: self.index_info.first().unwrap().clone(),
                index_value: self.index_values.first().unwrap().clone(),
                select_columns: Arc::new(DataSchema {
                    columns: self.select_columns,
                }),
                filters: self.filters,
                session: self.session.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    fn visit(&mut self, query: &Query) -> MySQLResult<()> {
        if query.limit.is_some() || !query.order_by.is_empty() {
            self.point_get = false;
            return Ok(());
        }
        let session = self.session.clone();
        let mut guard = session.lock().unwrap();
        match &query.body {
            SetExpr::Select(select) => self.visit_select(&mut *guard, select.as_ref())?,
            _ => {
                self.point_get = false;
            }
        }
        Ok(())
    }

    fn visit_select(&mut self, session: &mut Session, select: &Select) -> MySQLResult<()> {
        if select.top.is_some() {
            self.point_get = false;
            return Ok(());
        }
        if select.from.len() != 1 {
            self.point_get = false;
            return Ok(());
        }
        let table = select.from.first().unwrap();
        if !table.joins.is_empty() {
            self.point_get = false;
            return Ok(());
        }
        if let TableFactor::Table { name, .. } = &table.relation {
            if name.0.len() > 1 {
                let db = name.0.first().unwrap().value.to_lowercase();
                if !db.eq(session.get_db()) {
                    return Err(MySQLError::NoDB);
                }
            }
            let table_name = name.0.last().unwrap().value.to_lowercase();
            if let Some(t) = session.get_table(&table_name) {
                self.table = Some(t);
                if let Some(expr) = select.selection.as_ref() {
                    self.visit_selection(session, expr)?;
                }
                if self.index_info.is_empty() {
                    self.point_get = false;
                }
                if self.point_get {
                    self.visit_projections(&select.projection)?;
                }
            } else {
                return Err(MySQLError::NoTable(table_name));
            }
        }
        Ok(())
    }

    fn visit_projections(&mut self, projections: &[SelectItem]) -> MySQLResult<()> {
        let table = self.table.as_ref().unwrap();
        for p in projections {
            if let SelectItem::UnnamedExpr(Expr::Identifier(v)) = p {
                let col_name = v.value.to_lowercase();
                if let Some(col) = table.get_column(&col_name) {
                    self.select_columns.push(col);
                } else {
                    return Err(MySQLError::NoColumn);
                }
            } else {
                return Err(MySQLError::UnsupportSQL);
            }
        }
        Ok(())
    }

    fn visit_selection(&mut self, session: &mut Session, expr: &Expr) -> MySQLResult<()> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        self.visit_selection(session, left.as_ref())?;
                        self.visit_selection(session, right.as_ref())?;
                    }
                    BinaryOperator::Eq => match left.as_ref() {
                        Expr::Identifier(ident) => {
                            if let Expr::Value(v) = right.as_ref() {
                                self.select_index(
                                    &ident.value,
                                    EncodeValue::from_parse_value(v.clone())?,
                                )?;
                            }
                        }
                        Expr::Value(v) => {
                            if let Expr::Identifier(ident) = right.as_ref() {
                                self.select_index(
                                    &ident.value,
                                    EncodeValue::from_parse_value(v.clone())?,
                                )?;
                            }
                        }
                        _ => {}
                    },
                    _ => {
                        // TODO: Support other expression.
                    }
                }
            }
            _ => (),
        }
        Ok(())
    }

    fn select_index(&mut self, col_name: &String, value: EncodeValue) -> MySQLResult<()> {
        let name = col_name.to_lowercase();
        let table = self.table.as_ref().unwrap();
        if let Some(index) = table.get_index(&name) {
            if index.primary || index.unique {
                self.index_info.push(index);
                self.index_values.push(value);
            }
        } else if let Some(_col) = table.get_column(&name) {
            // TODO: add it to expression
        } else {
            return Err(MySQLError::NoColumn);
        }
        Ok(())
    }
}
