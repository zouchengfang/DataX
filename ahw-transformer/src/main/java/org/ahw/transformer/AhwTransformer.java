package org.ahw.transformer;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.transport.transformer.TransformerErrorCode;
import com.alibaba.datax.transformer.ComplexTransformer;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.ahw.transformer.support.Hiding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Liu Kun on 2019/3/14.
 */
public class AhwTransformer extends ComplexTransformer{
	private static final Logger LOG = LoggerFactory
            .getLogger(AhwTransformer.class);
    private Object masker;
    String maskMethodId = "";
    String key;
    int columnIndex;

    public AhwTransformer(){
        super.setTransformerName("ahw_transformer");
    }


    public Record evaluate(Record record, Map<String, Object> tContext, Object... paras){
        try {
        	LOG.info("========================================================");
            if (paras.length < 1) {
                throw new RuntimeException("Hiding transformer 缺少参数");
            }
            columnIndex = (Integer) paras[0];
            LOG.info("columnIndex="+columnIndex);
            LOG.info("ColumnNumber="+record.getColumnNumber());
            int temp = record.getColumnNumber();
            if(temp>0) {
            	for(int i = 0 ; i< temp;i++) {
            		Column cl = record.getColumn(i);
            		LOG.info("value,"+i+"="+cl.asString()+",type="+cl.getType());
            	}
            	LOG.info("");
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
       
        return record;
    }

}
