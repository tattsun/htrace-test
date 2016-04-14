package me.tattsun;

import me.tattsun.migration.HBaseMigration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;

/**
 * Created by tattsun on 2016/04/14.
 */
@Controller
public class GreetingController {

    @RequestMapping("/migrate")
    @ResponseBody
    public String migrate() throws IOException {
        HBaseMigration.migrate();
        return "OK";
    }

    @RequestMapping("/greeting")
    public String greeting(@RequestParam(value="name", required=false, defaultValue="World") String name,
                           Model model) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "testx");
        Put put = new Put(Bytes.toBytes("aab"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cdaou"), Bytes.toBytes("foeajoif"));
        table.put(put);

        model.addAttribute("name", name);
        return "greeting";
    }

}
