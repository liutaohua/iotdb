package org.apache.iotdb.db.qp.strategy.optimizer;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FromComponent;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.db.query.timegenerator.TagsGenerator;
import org.apache.iotdb.tsfile.read.expression.IExpression;

import java.io.IOException;
import java.util.*;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_WILDCARD;

public class RemoveTagsOptimizer implements ILogicalOptimizer {
  @Override
  public Operator transform(Operator operator, int fetchSize) throws QueryProcessException {
    if (!operator.isQuery()) {
      return operator;
    }

    QueryOperator queryOp = (QueryOperator) operator;
    FromComponent fromComponent = queryOp.getFromComponent();
    if (!fromComponent.isTags()) {
      return operator;
    }

    FilterOperator tagsFilterOperator = fromComponent.getTagsFilterOperator();
    IExpression iExpression = tagsFilterOperator.transformToExpression(Collections.emptyMap());
    TagsGenerator tagsGenerator = new TagsGenerator();

    try {
      tagsGenerator.constructNode(iExpression);
      List<ResultColumn> resultColumns = getTagsResultColumn(tagsGenerator);
      queryOp
          .getSelectComponent()
          .setResultColumns(
              removeStar(queryOp.getSelectComponent().getResultColumns(), resultColumns));

      FromComponent realFromComponent = getFromComponent(resultColumns);
      queryOp.setFromComponent(realFromComponent);
    } catch (Exception e) {
      throw new QueryProcessException(e.getMessage());
    }
    return queryOp;
  }

  private FromComponent getFromComponent(List<ResultColumn> resultColumns)
      throws IllegalPathException {
    FromComponent fromComponent = new FromComponent();

    HashSet<String> deduplicatePath = new HashSet<>();
    for (ResultColumn resultColumn : resultColumns) {
      if (deduplicatePath.add(resultColumn.getResultColumnName())) {
        PartialPath partialPath = new PartialPath(resultColumn.getResultColumnName());
        fromComponent.getPrefixPaths().add(partialPath.getDevicePath());
      }
    }
    return fromComponent;
  }

  private List<ResultColumn> getTagsResultColumn(TagsGenerator tagsGenerator)
      throws IOException, IllegalPathException {
    List<ResultColumn> resultPaths = new ArrayList<>();

    while (tagsGenerator.hasNext()) {
      ShowTimeSeriesResult next = (ShowTimeSeriesResult) tagsGenerator.nextObject();
      PartialPath partialPath = new PartialPath(next.getName());
      resultPaths.add(new ResultColumn(new TimeSeriesOperand(partialPath)));
    }

    return resultPaths;
  }

  private List<ResultColumn> removeStar(
      List<ResultColumn> originPaths, List<ResultColumn> resultColumns) {
    if (originPaths == null
        || originPaths.isEmpty()
        || resultColumns == null
        || resultColumns.isEmpty()) {
      return originPaths;
    }

    boolean containsStart = false;
    for (Iterator<ResultColumn> it = originPaths.iterator(); it.hasNext(); ) {
      ResultColumn resultColumn = it.next();
      if (PATH_WILDCARD.equals(resultColumn.getResultColumnName())) {
        it.remove();
        containsStart = true;
        continue;
      }
    }

    if (containsStart) {
      originPaths.addAll(resultColumns);
    }
    return originPaths;
  }
}
