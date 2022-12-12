package com.asuraflink.project.userPurchaseBehaviorTracker.function;

import com.alibaba.fastjson.JSON;
import com.asuraflink.project.userPurchaseBehaviorTracker.Launcher;
import com.asuraflink.project.userPurchaseBehaviorTracker.model.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class ConnectedBroadcastProcessFuntion extends KeyedBroadcastProcessFunction<String, UserEvent, Config, EvaluatedResult> {

    private Config defaultConfig = new Config("APP","2018-01-01",0,3);

    // (channel, Map<uid, UserEventContainer>)
    private final MapStateDescriptor<String, Map<String, UserEventContainer>> userMapStateDesc =
            new MapStateDescriptor<>(
                    "userEventContainerState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    new MapTypeInfo<>(String.class, UserEventContainer.class));

    @Override
    public void processElement(UserEvent value, ReadOnlyContext ctx, Collector<EvaluatedResult> out) throws Exception {
        String userId = value.getUserId();
        String channel = value.getChannel();

        EventType eventType = EventType.valueOf(value.getEventType());
        Config config = ctx.getBroadcastState(Launcher.configStateDescriptor).get(channel);
        log.info("Read config: channel = " + channel + ", config = " + config);
        if (Objects.isNull(config)) {
            config = defaultConfig;
        }

        final MapState<String, Map<String, UserEventContainer>> state =
                getRuntimeContext().getMapState(userMapStateDesc);

        // collect per-user events to the user map state
        Map<String, UserEventContainer> userEventContainerMap = state.get(channel);
        if (Objects.isNull(userEventContainerMap)) {
            userEventContainerMap = Maps.newHashMap();
            state.put(channel, userEventContainerMap);
        }
        if (!userEventContainerMap.containsKey(userId)) {
            UserEventContainer container = new UserEventContainer();
            container.setUserId(userId);
            userEventContainerMap.put(userId, container);
        }
        userEventContainerMap.get(userId).getUserEvents().add(value);

        // check whether a user purchase event arrives
        // if true, then compute the purchase path length, and prepare to trigger predefined actions
        if (eventType == EventType.PURCHASE) {
            log.info("Receive a purchase event: " + value);
            Optional<EvaluatedResult> result = compute(config, userEventContainerMap.get(userId));
            result.ifPresent(r -> out.collect(result.get()));
            // clear evaluated user's events
            state.get(channel).remove(userId);
        }
    }

    @Override
    public void processBroadcastElement(Config value, Context ctx, Collector<EvaluatedResult> out) throws Exception {
        String channel = value.getChannel();
        BroadcastState<String, Config> state = ctx.getBroadcastState(Launcher.configStateDescriptor);
        final Config oldConfig = ctx.getBroadcastState(Launcher.configStateDescriptor).get(channel);
        if(state.contains(channel)) {
            log.info("Configured channel exists: channel=" + channel);
            log.info("Config detail: oldConfig=" + oldConfig + ", newConfig=" + value);
        }else {
            log.info("Config detail: defaultConfig=" + defaultConfig + ", newConfig=" + value);
        }
        // update config value for configKey
        state.put(channel, value);
    }

    /**
     * 计算购买路径长度
     * @param config
     * @param container
     * @return
     */
    private Optional<EvaluatedResult> compute(Config config, UserEventContainer container) {
        Optional<EvaluatedResult> result = Optional.empty();
        String channel = config.getChannel();
        int historyPurchaseTimes = config.getHistoryPurchaseTimes();
        int maxPurchasePathLength = config.getMaxPurchasePathLength();

        int purchasePathLen = container.getUserEvents().size();
        //大于最大消费路径
        if (historyPurchaseTimes < 10 && purchasePathLen > maxPurchasePathLength) {
            // sort by event time
            container.getUserEvents().sort(Comparator.comparingLong(UserEvent::getEventTime));

            final Map<String, Integer> stat = Maps.newHashMap();
            container.getUserEvents()
                    .stream()
                    .collect(Collectors.groupingBy(UserEvent::getEventType))
                    .forEach((eventType, events) -> stat.put(eventType, events.size()));

            final EvaluatedResult evaluatedResult = new EvaluatedResult();
            evaluatedResult.setUserId(container.getUserId());
            evaluatedResult.setChannel(channel);
            evaluatedResult.setEventTypeCounts(stat);
            evaluatedResult.setPurchasePathLength(purchasePathLen);
            log.info("Evaluated result: " + JSON.toJSONString(evaluatedResult));
            result = Optional.of(evaluatedResult);
        }
        return result;
    }
}
