package com.scalpsecta.breakoutbot

import com.scalpsecta.liner.dto.TradingType
import com.scalpsecta.starter.autoconfigure.BinanceConfiguration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class LocalDependencyResolutionTest {
    @Test
    fun `loads liner dto and starter from local jars`() {
        assertThat(TradingType.FUTURES).isNotNull()
        assertThat(BinanceConfiguration.MARKET_DATA_BASE_URL_FUTURES)
            .startsWith("https://")
    }
}
