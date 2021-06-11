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
        try {
            queryOp.getSelectComponent().getResultColumns().clear();
            tagsGenerator.constructNode(iExpression);
            while (tagsGenerator.hasNext()) {
                ShowTimeSeriesResult next = (ShowTimeSeriesResult) tagsGenerator.nextObject();
                PartialPath partialPath = new PartialPath(next.getName());
                queryOp.getSelectComponent().addResultColumn(new ResultColumn(new TimeSeriesOperand(partialPath)));
                realFromComponent.getPrefixPaths().add(partialPath.getDevicePath());
            }
        } catch (Exception e) {
            throw new QueryProcessException(e.getMessage());
        }
        queryOp.setFromComponent(realFromComponent);
        return queryOp;
    }
}
