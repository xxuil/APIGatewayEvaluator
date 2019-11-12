package com.guba.test.gateway;

import com.guba.test.gateway.hadoop.GatewayRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Xiangxing Liu
 * @date 2019/11/12
 */
public class Evaluator  {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GatewayRunner(), args);
        System.exit(res);
    }
}
