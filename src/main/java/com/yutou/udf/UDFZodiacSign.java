package com.yutou.udf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 通过生日计算出星座UDF
 */
@Description(
        name = "zodiac",
        value = "_FUNC_(date)-from the input data string" +
                "or separate month and day arguments,return the sign of Zodiac.",
        extended = "Example:\n" +
                "> SELECT _FUNC_(date_string) FROM src;\n" +
                "> SELECT _FUNC_(month,day) FROM src;"
)
public class UDFZodiacSign extends UDF{
    private SimpleDateFormat df;

    public UDFZodiacSign() {
        df = new SimpleDateFormat("MM-dd-yyyy");
    }

    public String evaluate(Date date) {
        return evaluate(date.getMonth() + 1,date.getDate());
    }

    public String evaluate(String body) {
        Date date = null;
        try {
            date = df.parse(body);
        } catch (ParseException e) {
            return null;
        }
        return evaluate(date.getMonth() + 1,date.getDate());
    }

    public String evaluate(Integer month,Integer day) {
        if (month == 1) {
            if (day < 20) {
                return "Capricorn";
            } else {
                return "Aquarius";
            }
        }
        if (month == 2) {
            if (day < 19) {
                return "Aquarius";
            } else {
                return "Pisces";
            }
        }
        // and so on
        return null;
    }
}
