package com.hruan.study.spark.sql

import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.{DataType, StructType}

class StrictParser(parser: ParserInterface) extends ParserInterface {
  override def parsePlan(sqlText: String): LogicalPlan = {
    val logicalPlan = parser.parsePlan(sqlText)

    logicalPlan transform {
      case project @ Project(projectList, _) =>
        projectList.foreach {
          name => if (name.isInstanceOf[UnresolvedStar]) {
            throw new RuntimeException("you must specify your project column set, '*' is not allowed")
          }
        }
        project
    }
    logicalPlan
  }

  override def parseExpression(sqlText: String): Expression = parser.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier = parser.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = parser.parseFunctionIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType = parser.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType = parser.parseDataType(sqlText)
}