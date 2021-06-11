package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.FilterConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Map;

public class TagsFilterOperator extends BasicFunctionOperator {

  public TagsFilterOperator(FilterConstant.FilterType filterType, PartialPath path, String text) {
    super(filterType, path, text);
  }

  @Override
  protected Pair<IUnaryExpression, String> transformToSingleQueryFilter(
      Map<PartialPath, TSDataType> pathTSDataTypeHashMap) {
    IUnaryExpression ret = super.funcToken.getUnaryExpression(singlePath, (value.startsWith("'") && value.endsWith("'"))
            || (value.startsWith("\"") && value.endsWith("\""))
            ? value.substring(1, value.length() - 1): value);
    return new Pair<>(ret, singlePath.getFullPath());
  }
}
