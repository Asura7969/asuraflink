package com.asuraflink.sql.code

import org.apache.flink.api.common.functions.Function
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.table.planner.codegen.CodeGenUtils.{DEFAULT_INPUT1_TERM, boxedTypeTermForType, newName}
import org.apache.flink.table.planner.codegen.{CodeGenException, CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.planner.codegen.GenerateUtils.{generateInputAccess, generateLiteral}
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator.prepareOperands
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil.{ConstantLookupKey, FieldRefLookupKey, LookupKey}
import org.apache.flink.table.runtime.generated.{GeneratedCollector, GeneratedFunction}
import org.apache.flink.table.types.logical.{LogicalType, RowType}

import java.util

/**
 * @author asura7969
 * @create 2021-04-10-20:19
 */
object KeySelectorCodeGenerator {

  def generateFunction[K](
      ctx: CodeGeneratorContext,
      name: String,
      lookupKeys: util.Map[Integer, LookupKey],
      lookupKeyOrder: Array[Int],
      inputRowType: RowType,
      outputRowType: RowType,
      fieldCopy: Boolean): GeneratedKeySelector[K] = {

    val operands = prepareOperands(
      ctx,
      inputRowType,
      lookupKeys,
      lookupKeyOrder,
      fieldCopy)

    val funcName = newName(name)
    val inputTypeClass = boxedTypeTermForType(inputRowType)
    val outputTypeClass = boxedTypeTermForType(outputRowType)
    val funcCode =
      s"""
         |public class $funcName implements ${classOf[KeySelector[_, _]]} {
         |
         |    @Override
         |    public $outputTypeClass getKey($inputTypeClass value) throws Exception {
         |        $inputTypeClass $DEFAULT_INPUT1_TERM = ($inputTypeClass)value;
         |        ${ctx.reusePerRecordCode()}
         |        ${ctx.reuseInputUnboxingCode()}
         |        return
         |    }
         |
         |}
         |""".stripMargin

    new GeneratedKeySelector[K](funcName, funcCode, ctx.references.toArray)
  }

  private def prepareOperands(
     ctx: CodeGeneratorContext,
     inputType: LogicalType,
     lookupKeys: util.Map[Integer, LookupKey],
     lookupKeyOrder: Array[Int],
     fieldCopy: Boolean): Seq[GeneratedExpression] = {

    lookupKeyOrder
      .map(Integer.valueOf)
      .map(lookupKeys.get)
      .map {
        case constantKey: ConstantLookupKey =>
          generateLiteral(
            ctx,
            constantKey.dataType,
            constantKey.literal.getValue3)
        case fieldKey: FieldRefLookupKey =>
          generateInputAccess(
            ctx,
            inputType,
            DEFAULT_INPUT1_TERM,
            fieldKey.index,
            nullableInput = false,
            fieldCopy)
        case _ =>
          throw new CodeGenException("Invalid lookup key.")
      }
  }
}
