package com.github.viyadb.zeppelin;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterResult.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class ViyaDBInterpreter extends Interpreter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ViyaDBInterpreter.class);

    private String queryUrl;
    private Map<String, Future<?>> runningQueries = Collections.synchronizedMap(new HashMap<String, Future<?>>());

    public ViyaDBInterpreter(Properties props) {
        super(props);
    }

    @Override
    public void open() {
        queryUrl = "http://" + getProperty("viyadb.host") + ":"
                + getProperty("viyadb.port") + "/query";
    }

    @Override
    public void close() {
    }

    @Override
    public InterpreterResult interpret(String script, InterpreterContext context) {
        LOGGER.debug("Runing ViyaDB query: {}", script);

        InterpreterResult result = new InterpreterResult(Code.SUCCESS);
        if (!StringUtils.isEmpty(script)) {
            try {
                Future<HttpResponse<String>> future = Unirest.post(queryUrl)
                        .header("Content-Type", "application/json")
                        .body(script)
                        .asStringAsync();
                runningQueries.put(context.getParagraphId(), future);

                HttpResponse<String> response = future.get();
                if (response.getStatus() == 200) {
                    result.add(Type.TABLE, response.getBody());
                } else {
                    result = new InterpreterResult(Code.ERROR,
                            "Bad response received from ViyaDB instance: " + response.getBody());
                }
            } catch (Throwable e) {
                LOGGER.error("Error querying ViyaDB script in paragraph {}", context.getParagraphId(), e);
                result = new InterpreterResult(Code.ERROR, e.getMessage());

            } finally {
                runningQueries.remove(context.getParagraphId());
            }
        }
        return result;
    }

    @Override
    public void cancel(InterpreterContext context) {
        Future<?> future = runningQueries.get(context.getParagraphId());
        if (future != null) {
            future.cancel(true);
        }
    }

    @Override
    public FormType getFormType() {
        return FormType.SIMPLE;
    }

    @Override
    public int getProgress(InterpreterContext context) {
        return 0;
    }
}
