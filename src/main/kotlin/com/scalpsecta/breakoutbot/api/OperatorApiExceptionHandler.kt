package com.scalpsecta.breakoutbot.api

import com.scalpsecta.breakoutbot.level.LevelException
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.server.ServerWebInputException

@RestControllerAdvice
class OperatorApiExceptionHandler {
    @ExceptionHandler(LevelException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    fun levelError(error: LevelException): ApiErrorResponse =
        ApiErrorResponse(
            code = error.code,
            message = error.message ?: error.code.name,
        )

    @ExceptionHandler(ServerWebInputException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    fun invalidRequest(
        @Suppress("UNUSED_PARAMETER") error: ServerWebInputException,
    ): ApiErrorResponse =
        ApiErrorResponse(
            code = LevelReasonCode.INVALID_LEVEL,
            message = "Level request is malformed",
        )
}

data class ApiErrorResponse(
    val code: LevelReasonCode,
    val message: String,
)
