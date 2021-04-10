package com.asuraflink.sql.code

import org.apache.flink.api.common.functions.Function
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.table.planner.codegen.CodeGenUtils.{boxedTypeTermForType, newName}
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.runtime.generated.{GeneratedCollector, GeneratedFunction}
import org.apache.flink.table.types.logical.RowType

/**
 * @author asura7969
 * @create 2021-04-10-20:19
 */
object KeySelectorCodeGenerator {

  def generateFunction[K](
      ctx: CodeGeneratorContext,
      name: String,
      inputRowType: RowType,
      outputRowType: RowType): GeneratedKeySelector[K] = {

    val funcName = newName(name)
    val inputTypeClass = boxedTypeTermForType(inputRowType)
    val outputTypeClass = boxedTypeTermForType(outputRowType)
    val funcCode =
      s"""
         |public class $funcName implements ${classOf[KeySelector[_, _]]} {
         |
         |    @Override
         |    public $outputTypeClass getKey($inputTypeClass value) throws Exception {
         |        $inputTypeClass _in = ($inputTypeClass)value;
         |
         |        return
         |    }
         |
         |}
         |""".stripMargin

    new GeneratedKeySelector[K](funcName, funcCode, ctx.references.toArray)
  }
}
