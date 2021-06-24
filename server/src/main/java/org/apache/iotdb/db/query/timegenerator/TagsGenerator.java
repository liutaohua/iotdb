package org.apache.iotdb.db.query.timegenerator;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.AndNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.Node;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.OrNode;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TagsGenerator extends TimeGenerator {

  @Override
  protected IBatchReader generateNewBatchReader(SingleSeriesExpression expression)
      throws IOException {
    return null;
  }

  @Override
  protected boolean isAscending() {
    return false;
  }

  @Override
  protected Node construct(IExpression expression) throws IOException {
    if (expression.getType() == ExpressionType.SERIES) {
      SingleSeriesExpression singleSeriesExp = (SingleSeriesExpression) expression;
      Path indexName = singleSeriesExp.getSeriesPath();

      UnaryFilter filter = (UnaryFilter) singleSeriesExp.getFilter();
      try {
        List<ShowTimeSeriesResult> showTimeSeriesResults =
            generateAllTagsPaths(indexName, filter.getValue());
        // put the current reader to valueCache
        TagsLeafNode leafNode = new TagsLeafNode(showTimeSeriesResults);
        leafNodeCache.computeIfAbsent(indexName, p -> new ArrayList<>()).add(leafNode);

        return leafNode;
      } catch (MetadataException e) {
        throw new IOException(e);
      }
    } else {
      Node leftChild = construct(((IBinaryExpression) expression).getLeft());
      Node rightChild = construct(((IBinaryExpression) expression).getRight());

      if (expression.getType() == ExpressionType.OR) {
        hasOrNode = true;
        return new OrNode(leftChild, rightChild, isAscending());
      } else if (expression.getType() == ExpressionType.AND) {
        return new AndNode(leftChild, rightChild);
      }
      throw new UnSupportedDataTypeException(
          "Unsupported ExpressionType when construct OperatorNode: " + expression.getType());
    }
  }

  private List<ShowTimeSeriesResult> generateAllTagsPaths(Path index, Comparable value)
      throws MetadataException {
    ShowTimeSeriesPlan showTimeSeriesPlan = new ShowTimeSeriesPlan(new PartialPath("root"), 0, 0);
    showTimeSeriesPlan.setIsContains(false);
    showTimeSeriesPlan.setKey(index.getFullPath());
    showTimeSeriesPlan.setValue(String.valueOf(value));
    showTimeSeriesPlan.setOrderByHeat(false);
    List<ShowTimeSeriesResult> showTimeSeriesResults =
        IoTDB.metaManager.getInstance().showTimeseries(showTimeSeriesPlan, new QueryContext());
    showTimeSeriesResults.sort(ShowTimeSeriesResult::compareTo);
    return showTimeSeriesResults;
  }
}
