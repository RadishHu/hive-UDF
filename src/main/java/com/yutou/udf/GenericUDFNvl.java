package com.yutou.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * 实现nvl函数
 * 当传入null时，返回默认的值
 */
@Description(
        name = "nvl",
        value = "_FUNC_（value,default_value - Return default value if value is null else return value",
        extended = "Example:\n" +
                "> SELECT _FUNC_(null,'bla') FROM src LIMIT 1;\n"
)
public class GenericUDFNvl extends GenericUDF {

    private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    private ObjectInspector[] argumentOIs;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        argumentOIs = arguments;
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("the operator NVL accept 2 arguments.");
        }
        returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

        if (!(returnOIResolver.update(arguments[0]) && returnOIResolver.update(arguments[1]))) {
            throw new UDFArgumentTypeException(2,"The 1st and 2nd args of function NVL should have the same type," +
                    "but they are different:" + arguments[0].getTypeName() + "and " + arguments[1].getTypeName());
        }
        return returnOIResolver.get();
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object retVal = returnOIResolver.convertIfNecessary(arguments[0].get(), argumentOIs[0]);
        if (retVal == null) {
            retVal = returnOIResolver.convertIfNecessary(arguments[1].get(),argumentOIs[1]);
        }
        return retVal;
    }

    @Override
    public String getDisplayString(String[] strings) {
        StringBuilder sb = new StringBuilder();
        sb.append("if");
        sb.append(strings[0]);
        sb.append("is null");
        sb.append("returns");
        sb.append(strings[1]);
        return sb.toString();
    }
}
