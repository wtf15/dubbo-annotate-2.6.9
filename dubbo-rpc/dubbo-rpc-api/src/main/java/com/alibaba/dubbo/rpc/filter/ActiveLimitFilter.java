/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.rpc.filter;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcStatus;

/**
 * LimitInvokerFilter
 */
@Activate(group = Constants.CONSUMER, value = Constants.ACTIVES_KEY)
public class ActiveLimitFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        URL url = invoker.getUrl();
        String methodName = invocation.getMethodName();
        // 获取最大并发数
        int max = invoker.getUrl().getMethodParameter(methodName, Constants.ACTIVES_KEY, 0);
        RpcStatus count = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName());
        if (max > 0) {
            // 获取超时时间
            long timeout = invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.TIMEOUT_KEY, 0);
            long start = System.currentTimeMillis();
            long remain = timeout;
            // 获取当前并发数
            int active = count.getActive();
            if (active >= max) {
                synchronized (count) {
                    // 加锁，并循环获取当前并发数,如果大于限量阈值则等待
                    while ((active = count.getActive()) >= max) {
                        try {
                            count.wait(remain);
                        } catch (InterruptedException e) {
                        }
                        long elapsed = System.currentTimeMillis() - start;
                        // 剩余等待时间
                        remain = timeout - elapsed;
                        if (remain <= 0) {
                            throw new RpcException("Waiting concurrent invoke timeout in client-side for service:  "
                                    + invoker.getInterface().getName() + ", method: "
                                    + invocation.getMethodName() + ", elapsed: " + elapsed
                                    + ", timeout: " + timeout + ". concurrent invokes: " + active
                                    + ". max concurrent invoke limit: " + max);
                        }
                    }
                }
            }
        }
        try {
            // 当前并发数低于限流阈值，则会从上面的while循环跳出并来到这里
            long begin = System.currentTimeMillis();
            // 并发计数器原子+1
            RpcStatus.beginCount(url, methodName);
            try {
                // 执行 Invoker调用
                Result result = invoker.invoke(invocation);
                // 调用结束，并发计数器原子-1
                RpcStatus.endCount(url, methodName, System.currentTimeMillis() - begin, true);
                return result;
            } catch (RuntimeException t) {
                RpcStatus.endCount(url, methodName, System.currentTimeMillis() - begin, false);
                throw t;
            }
        } finally {
            // 当前请求已经结束，通过notify唤醒另外一个线程
            if (max > 0) {
                synchronized (count) {
                    count.notify();
                }
            }
        }
    }

}
