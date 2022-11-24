package bean;

import lombok.Data;

/**
 * @ClassName TableProcess
 * @Description
 * @Author Lhr
 * @Date 2022/11/23 14:37
 */
@Data
public class TableProcess {
    /**来源表*/
    String sourceTable;
    /**输出表*/
    String sinkTable;
    /**输出字段*/
    String sinkColumns;
    /**主键字段*/
    String sinkPk;
    /**建表扩展*/
    String sinkExtend;
}
