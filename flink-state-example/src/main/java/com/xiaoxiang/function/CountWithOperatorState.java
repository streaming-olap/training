package com.xiaoxiang.function;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/*
 *
 *   假设事件场景为某业务事件流中有事件 1、2、3、4、5、6、7、8、9 ......
 *
 *   现在，想知道两次事件1之间，一共发生了多少次其他的事件，分别是什么事件，然后输出相应结果。
 *   如下:
 *    事件流 1 2 3 4 5 1 3 4 5 6 7 1 4 5 3 9 9 2 1 ....
 *    输出  4     2 3 4 5
 *          5     3 4 5 6 7
 *          8     4 5 3 9 9 2
 *
 */
public class CountWithOperatorState extends RichFlatMapFunction<Long,String> implements CheckpointedFunction {
    /*
     *  保存结果状态
     */
    private transient ListState<Long>  checkPointCountList;
    private List<Long> listBufferElements;

    @Override
    public void flatMap(Long r, Collector<String> collector) throws Exception {
        if (r == 1) {
            if (listBufferElements.size() > 0) {
                StringBuffer buffer = new StringBuffer();
                for(int i = 0 ; i < listBufferElements.size(); i ++) {
                    buffer.append(listBufferElements.get(i) + " ");
                }
                collector.collect(buffer.toString());
                listBufferElements.clear();
            }
        } else {
            listBufferElements.add(r);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkPointCountList.clear();
        for (int i = 0 ; i < listBufferElements.size(); i ++) {
            checkPointCountList.add(listBufferElements.get(i));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Long> listStateDescriptor =
                new ListStateDescriptor<Long>(
                        "listForThree",
                        TypeInformation.of(new TypeHint<Long>() {}));

        checkPointCountList = functionInitializationContext.getOperatorStateStore().getListState(listStateDescriptor);
        if (functionInitializationContext.isRestored()) {
            for (Long element : checkPointCountList.get()) {
                listBufferElements.add(element);
            }
        }
    }
}
