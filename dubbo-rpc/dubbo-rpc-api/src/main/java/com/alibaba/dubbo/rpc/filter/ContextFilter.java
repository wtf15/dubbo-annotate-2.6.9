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
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.alibaba.dubbo.rpc.RpcResult;

import java.util.HashMap;
import java.util.Map;

/**
 * ContextInvokerFilter
 */
@Activate(group = Constants.PROVIDER, order = -10000)
public class ContextFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        Map<String, String> attachments = invocation.getAttachments();
        // 清除异步属性。防止异步属性传到过滤器链的下一个环节
        if (attachments != null) {
            attachments = new HashMap<String, String>(attachments);
            attachments.remove(Constants.PATH_KEY);
            attachments.remove(Constants.GROUP_KEY);
            attachments.remove(Constants.VERSION_KEY);
            attachments.remove(Constants.DUBBO_VERSION_KEY);
            attachments.remove(Constants.TOKEN_KEY);
            attachments.remove(Constants.TIMEOUT_KEY);
            attachments.remove(Constants.ASYNC_KEY);// Remove async property to avoid being passed to the following invoke chain.
        }
        // 设置当前请求的上下文
        RpcContext.getContext()
                .setInvoker(invoker)
                .setInvocation(invocation)
//                .setAttachments(attachments)  // merged from dubbox
                .setLocalAddress(invoker.getUrl().getHost(),
                        invoker.getUrl().getPort());

        // mreged from dubbox
        // we may already added some attachments into RpcContext before this filter (e.g. in rest protocol)
        if (attachments != null) {
            if (RpcContext.getContext().getAttachments() != null) {
                RpcContext.getContext().getAttachments().putAll(attachments);
            } else {
                RpcContext.getContext().setAttachments(attachments);
            }
        }

        if (invocation instanceof RpcInvocation) {
            ((RpcInvocation) invocation).setInvoker(invoker);
        }
        try {
            // 调用过滤器链的下一个节点
            RpcResult result = (RpcResult) invoker.invoke(invocation);
            // pass attachments to result
            result.addAttachments(RpcContext.getServerContext().getAttachments());
            return result;
        } finally {
            // 清除上下文信息。对于异步调用的场景，即使是同一个线程，处理不同的请求也会创
            // 建一个新的RpcContext对象。因此调用完成后，需要清理对应的上下文信息
            RpcContext.removeContext();
            RpcContext.getServerContext().clearAttachments();
        }
    }
}
