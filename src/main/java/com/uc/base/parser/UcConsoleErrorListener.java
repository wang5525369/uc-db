package com.uc.base.parser;

import org.antlr.v4.runtime.ConsoleErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

public class UcConsoleErrorListener extends ConsoleErrorListener {
    private String errorMsg = "";
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
        errorMsg = "行: " + line + " 列:" + charPositionInLine + " 错误:" + msg;
    }

    public String getErrorMsg() {
        return errorMsg;
    }
}
