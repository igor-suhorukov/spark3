package com.github.igorsuhorukov.arrow.spark;

import lombok.experimental.UtilityClass;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import scala.Option;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@UtilityClass
public class SchemaUtils {
    public static StructType sparkSchema(Schema arrowSchema){
        List<Field> fields = arrowSchema.getFields();
        StructField[] structFields = fields.stream().map(field -> {
            Map<String, String> metadata = field.getMetadata();
            MetadataBuilder metadataBuilder = new MetadataBuilder();
            metadata.forEach(metadataBuilder::putString);
            return new StructField(field.getName(),
                    ArrowUtils.fromArrowField(field),
                    field.isNullable(),
                    metadata.isEmpty() ? Metadata.empty() : metadataBuilder.build());
        }).toArray(StructField[]::new);
        return new StructType(structFields);
    }

    public static Schema arrowSchema(StructType sparkSchema){
        List<StructField> fields = Arrays.asList(sparkSchema.fields());
        return new Schema(fields.stream().
                map(structField -> {
                    Field field = ArrowUtils.toArrowField(
                            structField.name(), structField.dataType(), structField.nullable(), "UTC");
                    Option<String> comment = structField.getComment();
                    if (!comment.isDefined() || comment.isEmpty()) {
                        return field;
                    } else {
                        FieldType fieldType = field.getFieldType();
                        Map<String, String> metadata = new HashMap<>(field.getMetadata());
                        metadata.put("comment", comment.get());
                        return new Field(field.getName(),new FieldType(fieldType.isNullable(),
                                fieldType.getType(), field.getDictionary(), metadata), field.getChildren());
                    }
                }).
                collect(Collectors.toList()));
    }

    public static String sparkDDLSchema(Schema arrowSchema){
        return sparkSchema(arrowSchema).toDDL();
    }
}
