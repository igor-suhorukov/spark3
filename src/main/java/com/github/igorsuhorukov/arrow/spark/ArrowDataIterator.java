package com.github.igorsuhorukov.arrow.spark;

import lombok.Getter;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.Analyzer;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.jetbrains.annotations.NotNull;
import scala.collection.immutable.Seq;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class ArrowDataIterator implements Iterator<Row> {
    @Getter
    private final StructType schema;
    private final ExpressionEncoder.Deserializer<Row> deserializer;
    private final Iterator<InternalRow> internalRowIterator;

    public ArrowDataIterator(VectorSchemaRoot vectorSchemaRoot) {
        validateArrowData(vectorSchemaRoot);
        this.schema = SchemaUtils.sparkSchema(vectorSchemaRoot.getSchema());

        List<FieldVector> fieldVectors = Objects.requireNonNull(vectorSchemaRoot).getFieldVectors();
        ArrowColumnVector[] arrowColumnVectors = fieldVectors.stream().map(ArrowColumnVector::new)
                                                    .toArray(ArrowColumnVector[]::new);
        ColumnarBatch columnarBatch = new ColumnarBatch(arrowColumnVectors, vectorSchemaRoot.getRowCount());
        internalRowIterator = columnarBatch.rowIterator();

        ExpressionEncoder<Row> sourceEncoder = RowEncoder.apply(this.schema);
        ExpressionEncoder<Row> rowExpressionEncoder = sourceEncoder.resolveAndBind(
                (Seq<Attribute>) sourceEncoder.resolveAndBind$default$1(),
                (Analyzer) sourceEncoder.resolveAndBind$default$2());
        deserializer = rowExpressionEncoder.createDeserializer();
    }

    @Override
    public boolean hasNext() {
        return internalRowIterator.hasNext();
    }

    @Override
    public Row next() {
        InternalRow record = internalRowIterator.next();
        return deserializer.apply(record);
    }

    @NotNull
    private static VectorSchemaRoot validateArrowData(VectorSchemaRoot vectorSchemaRoot) {
        return Objects.requireNonNull(vectorSchemaRoot, "Expected non null vectorSchemaRoot (apache arrow vector data)");
    }

    public static ArrowDataIterator returnSparkIteratorAndDeallocateAfterExecution(
                                VectorSchemaRoot vectorSchemaRoot, DeallocateArrowResourcs deallocateArrowResources){
        Objects.requireNonNull(deallocateArrowResources, "Expected non null deallocateArrowResources");
        validateArrowData(vectorSchemaRoot);
        TaskContext.get().addTaskCompletionListener(deallocateArrowResources);
        return new ArrowDataIterator(vectorSchemaRoot);
    }
}
