package com.github.igorsuhorukov.arrow.spark;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskCompletionListener;

@AllArgsConstructor
public class DeallocateArrowResourcs implements TaskCompletionListener {
    private final VectorSchemaRoot vectorSchemaRoot;
    private final ArrowFileReader arrowFileReader;
    private final BufferAllocator allocator;

    public DeallocateArrowResourcs(VectorSchemaRoot vectorSchemaRoot, BufferAllocator allocator) {
        this(vectorSchemaRoot, null, allocator);
    }

    @Override
    @SneakyThrows
    public void onTaskCompletion(TaskContext context) {
        AutoCloseables.close(vectorSchemaRoot, arrowFileReader, allocator);
    }
}
