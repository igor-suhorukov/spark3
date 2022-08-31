package com.github.igorsuhorukov.arrow.spark;

import lombok.experimental.UtilityClass;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@UtilityClass
public class SchemaUtils {
    public static StructType sparkSchema(Schema arrowSchema){
        List<Field> fields = arrowSchema.getFields();
        StructField[] structFields = fields.stream().map(field ->
                new StructField(field.getName(),
                                ArrowUtils.fromArrowField(field),
                                field.isNullable(),
                                Metadata.empty()/*todo fill metadata */)).toArray(StructField[]::new);
        return new StructType(structFields);
    }

    public static Schema arrowSchema(StructType sparkSchema){
        List<StructField> fields = Arrays.asList(sparkSchema.fields());
        return new Schema(fields.stream().
                map(structField -> ArrowUtils.toArrowField(
                        structField.name(),structField.dataType(),structField.nullable(),"UTC")).
                collect(Collectors.toList()));
    }

    public static String sparkDDLSchema(Schema arrowSchema){
        return sparkSchema(arrowSchema).toDDL();
    }
}
