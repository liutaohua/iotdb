package org.apache.iotdb.db.qp.strategy.optimizer;

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

import java.util.Collections;
import java.util.HashSet;

public class RemoveTagsOptimizer implements ILogicalOptimizer {
    @Override
    public Operator transform(Operator operator, int fetchSize) throws QueryProcessException {
        if (!operator.isQuery()) {
            return null;
        }

        QueryOperator queryOp = (QueryOperator) operator;
        FromComponent fromComponent = queryOp.getFromComponent();
        if (!fromComponent.isTags()) {
            return null;
        }

        FromComponent realFromComponent = new FromComponent();
        FilterOperator tagsFilterOperator = fromComponent.getTagsFilterOperator();
        IExpression iExpression = tagsFilterOperator.transformToExpression(Collections.emptyMap());
        TagsGenerator tagsGenerator = new TagsGenerator();

        HashSet deduplicatePath = new HashSet();
        HashSet deduplicateSelect = new HashSet();
        try {
            queryOp.getSelectComponent().getResultColumns().clear();
            tagsGenerator.constructNode(iExpression);
            while (tagsGenerator.hasNext()) {
                ShowTimeSeriesResult next = (ShowTimeSeriesResult) tagsGenerator.nextObject();
                if (!deduplicatePath.contains(next.getName())) {
                    deduplicatePath.add(next.getName());
                    PartialPath partialPath = new PartialPath(next.getName());

                    String measurement = partialPath.getMeasurement();
                    if (!deduplicateSelect.contains(measurement)) {
                        deduplicateSelect.add(measurement);
                        queryOp.getSelectComponent()
                                .addResultColumn(
                                        new ResultColumn(new TimeSeriesOperand(new PartialPath(measurement))));
                    }
                    realFromComponent.getPrefixPaths().add(partialPath.getDevicePath());
                }
            }
            realFromComponent.getPrefixPaths().sort(Comparable::compareTo);
        } catch (Exception e) {
            throw new QueryProcessException(e.getMessage());
        }
        queryOp.setFromComponent(realFromComponent);
        return queryOp;
    }
}
