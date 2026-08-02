package com.scalpsecta.breakoutbot.api

import com.scalpsecta.breakoutbot.control.OperatorCommandException
import com.scalpsecta.breakoutbot.level.LevelException
import com.scalpsecta.breakoutbot.level.LevelReasonCode
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.server.ServerWebInputException
import org.springframework.web.server.ServerWebExchange

@RestControllerAdvice
class OperatorApiExceptionHandler {
    @ExceptionHandler(LevelException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    fun levelError(error: LevelException): ApiErrorResponse =
        ApiErrorResponse(
            code = error.code.name,
            message = error.message ?: error.code.name,
        )

    @ExceptionHandler(OperatorCommandException::class)
    @ResponseStatus(HttpStatus.CONFLICT)
    fun commandError(error: OperatorCommandException): ApiErrorResponse =
        ApiErrorResponse(
            code = error.code.name,
            message = error.message ?: error.code.name,
        )

    @ExceptionHandler(ServerWebInputException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    fun invalidRequest(
        @Suppress("UNUSED_PARAMETER") error: ServerWebInputException,
        exchange: ServerWebExchange,
    ): ApiErrorResponse {
        val levelRequest = exchange.request.path.value().startsWith("/api/levels")
        return ApiErrorResponse(
            code = if (levelRequest) {
                LevelReasonCode.INVALID_LEVEL.name
            } else {
                INVALID_COMMAND_CODE
            },
            message = if (levelRequest) {
                "Level request is malformed"
            } else {
                "Operator command request is malformed"
            },
        )
    }
}

data class ApiErrorResponse(
    val code: String,
    val message: String,
)

private const val INVALID_COMMAND_CODE = "INVALID_COMMAND"
