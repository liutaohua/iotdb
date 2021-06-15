package org.apache.iotdb.db.query.timegenerator;

import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.LeafNode;
import org.apache.iotdb.tsfile.read.query.timegenerator.node.NodeType;

import java.io.IOException;
import java.util.List;

public class TagsLeafNode extends LeafNode {

  private List<ShowTimeSeriesResult> showTimeSeriesResults;

  public TagsLeafNode() {
    super(null);
  }

  public TagsLeafNode(List<ShowTimeSeriesResult> showTimeSeriesResults) {
    super(null);
    this.showTimeSeriesResults = showTimeSeriesResults;
  }

  @Override
  public boolean hasNext() throws IOException {
    return !showTimeSeriesResults.isEmpty();
  }

  @Override
  public long next() throws IOException {
    return 0;
  }

  @Override
  public Comparable nextObject() throws IOException {
    return showTimeSeriesResults.remove(0);
  }

  @Override
  public NodeType getType() {
    return null;
  }
}
