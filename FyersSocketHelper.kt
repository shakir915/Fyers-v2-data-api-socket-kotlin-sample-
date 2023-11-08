package shakir.bhav.common

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import io.ktor.http.*
import io.ktor.websocket.*
import kotlinx.coroutines.delay
import org.json.JSONArray
import org.json.JSONObject
import shakir.bhav.common.chartUiCommon.NSECrawler
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.math.absoluteValue


data class FyersSocketKlines(
    val symbol: String,
    val o: MutableList<Float>,
    val h: MutableList<Float>,
    val l: MutableList<Float>,
    val c: MutableList<Float>,
    val v: MutableList<Long>,
    val t: MutableList<Long>,
    val tOG: MutableList<Long>,
    val invalidate: () -> Unit,
)


object FyersSocketHelper {


    var process_MCCSH_UI_LOGIC = false
    var process_MCCSH_UI_LOGIC_function: (() -> Unit)? = null

    var klines: ArrayList<FyersSocketKlines> = arrayListOf()


    var fyersSocket: DefaultClientWebSocketSession? = null


    val httpClient: HttpClient by lazy {
        HttpClient(CIO) {
            install(WebSockets) {
                // Configure WebSockets
            }
        }
    }

    var lastHITTime = 0L


    suspend fun subscribe() {
        try {

            if (System.currentTimeMillis() - lastHITTime <= TimeUnit.SECONDS.toMillis(10)) {

                subscribeReal()
            } else {
                startFyersocket()
            }

        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    var listSubscribedList = arrayListOf<String>()
    suspend fun subscribeReal() {
        val symbolToFyersSymbol = FyersMaster.symbolToFyersSymbol()
        fyersSocket?.send(JSONObject().apply {
            put("T", "SUB_L2")
            put("L2LIST", JSONArray().apply {
                /** : Please note :  `PutAll` crash in android : use `put` */
                klines.map { it.symbol } + Series.getMyListScrips("intra_100").distinct().filterNotNull().take(100).apply {
                    listSubscribedList.clear()
                    listSubscribedList.addAll(this)
                }.map {

                    symbolToFyersSymbol.get(it)
                }/*.filter { it.contains("RELIANCE") }*/.forEach {
                    put(it)
                }

            })
            put("SUB_T", "1")
            println("startFyersocket subscribeReal data " + this.getJSONArray("L2LIST").length() + " " + this.toString())
        }.toString())
    }

    suspend fun close() {
        try {
            fyersSocket?.close()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }


    suspend fun startFyersocket() {
        if (settings.FYERS__access_token.isNullOrBlank()) {
            println("startFyersocket Not Logged In")
            return
        }


        val idToSymbol = FyersMaster.fyersIDtoSymbol()


        httpClient.wss(
            method = HttpMethod.Get,
            host = "api.fyers.in",
            port = 443,
            path = "/socket/v2/dataSock?access_token=${settings.FYERS_APP_ID}:${settings.FYERS__access_token}"
        ) {
            try {
                fyersSocket = this
                val file = getDataFolder("FyersTest", "Fyers__${milliFileDateTime(System.currentTimeMillis())}")
                val text1 = (incoming.receive() as? Frame.Text)?.readText()
                println("startFyersocket " + text1)
                try {
                    if (JSONObject(text1).getInt("code") == -1600) {
                        FyersAPIHelper.logout()
                        FyersSocketHelper.close()
                    }

                    if (JSONObject(text1).getInt("code") == -1200) {
                        NSECrawler.fetchFyersSymbolMaster()
                        FyersSocketHelper.close()
                        FyersSocketHelper.startFyersocket()
                    }


                } catch (e: Exception) {
                    e.printStackTrace()
                }


                // https://unpkg.com/browse/extra-fyers@2.0.0/index.js

                subscribeReal()
                println("startFyersocket " + (incoming.receive() as? Frame.Text)?.readText())
                while (true) {
                    val othersMessage = incoming.receive() as? Frame.Binary ?: continue
                    val binary = othersMessage.readBytes()
                    file.appendBytes(Ints.toByteArray(binary.size))
                    file.appendBytes(binary)
                    decode(binary, idToSymbol)
                    lastHITTime = System.currentTimeMillis()

                }
            } catch (e: Exception) {
                e.printStackTrace()
            }

        }
    }


    val type_L1_7208 = 7208


    fun tryDecode() {


        val symbolMaps = FyersMaster.fyersIDtoSymbol()

        val ba = getDataFolder("FyersTest", "Fyers__2022_11_17__10_21_40").readBytes()
        var ib = 0
        while (true) {
            try {
                val length = Ints.fromByteArray(ba.sliceArray(ib..ib + 3))
                ib += 4
                val ca = ba.sliceArray(ib until ib + length)
                decode(ca, symbolMaps)
                ib += length
                //if (ib >= 10000) break


            } catch (e: Exception) {
                e.printStackTrace()
                break
            }
        }

        list.toList().sortedByDescending { it.second }.forEachIndexed { index, pair ->
            println("${pair.first} ${pair.second} $index ")
        }


    }


    val list = HashMap<String, Int>()


    fun decode(ca: ByteArray, idToSymbol: HashMap<String, String>) {

        var logs = ""

        val token = Longs.fromByteArray(ca.sliceArray(0 until 8)).toString()
        val tt = Ints.fromByteArray(ca.sliceArray(8 until 12))
        val fyCode = Shorts.fromByteArray(ca.sliceArray(12 until 14)).toInt()
        val marketStat = Shorts.fromByteArray(ca.sliceArray(14 until 16))
        val pktlen = Shorts.fromByteArray(ca.sliceArray(16 until 18))
        val L2 = ca.sliceArray(18 until 19)
        val hasOi = fyCode == 7202 || fyCode == 31038
        val symbol = idToSymbol.get(token) ?: return

        if (list.get(symbol) == null) {
            list.put(symbol, 1)
        } else {
            list.put(symbol, list.get(symbol)!!.plus(1))
        }

        val l1Off = if (hasOi) 88 else 72
        if (fyCode == 7202) {
            //todo writeOiData
        } else {
            //writeCommonData
            val price_conv = Ints.fromByteArray(ca.sliceArray(24 until 28)).toLong()
            val ltp = Ints.fromByteArray(ca.sliceArray(28 until 32)).toLong().toDouble().div(price_conv)
            val open_price = Ints.fromByteArray(ca.sliceArray(32 until 36)).toLong().toDouble().div(price_conv)
            val high_price = Ints.fromByteArray(ca.sliceArray(36 until 40)).toLong().toDouble().div(price_conv)
            val low_price = Ints.fromByteArray(ca.sliceArray(40 until 44)).toLong().toDouble().div(price_conv)
            val close_price = Ints.fromByteArray(ca.sliceArray(48 until 52)).toLong().toDouble().div(price_conv)
            val o = Ints.fromByteArray(ca.sliceArray(52 until 56)).toLong().toDouble().div(price_conv)
            val h = Ints.fromByteArray(ca.sliceArray(56 until 60)).toLong().toDouble().div(price_conv)
            val l = Ints.fromByteArray(ca.sliceArray(60 until 64)).toLong().toDouble().div(price_conv)
            val c = Ints.fromByteArray(ca.sliceArray(64 until 68)).toLong().toDouble().div(price_conv)
            //"c" (close) always zero when tested; use "close_price"
            val v = Longs.fromByteArray(ca.sliceArray(68 until 76)).toULong()
            val vS = Longs.fromByteArray(ca.sliceArray(68 until 76)).toInt()
            if (hasOi) {
                val oi = Longs.fromByteArray(ca.sliceArray(76 until 84)).toULong()
                val pdoi = Longs.fromByteArray(ca.sliceArray(84 until 92)).toULong()
            }


            println("$symbol o $o h$h l$l c$c p$ltp")


            if (Shorts.fromByteArray(ca.sliceArray(12..13)).toInt() == type_L1_7208) {
                val LTQ = Ints.fromByteArray(ca.sliceArray(l1Off until l1Off + 4)).toLong()
                val LTT = Ints.fromByteArray(ca.sliceArray(l1Off + 4 until l1Off + 8)).toLong().times(1000)
                val AvgTP = Ints.fromByteArray(ca.sliceArray(l1Off + 8 until l1Off + 12)).toLong().toDouble().div(100)
                val volumeToday = Ints.fromByteArray(ca.sliceArray(l1Off + 12 until l1Off + 16)).toLong()
                val tot_buy = Longs.fromByteArray(ca.sliceArray(l1Off + 16 until l1Off + 24)).toULong()
                val tot_sell = Longs.fromByteArray(ca.sliceArray(l1Off + 24 until l1Off + 32)).toULong()
                val market_pic_price = Ints.fromByteArray(ca.sliceArray(l1Off + 32 until l1Off + 36)).toLong().toDouble().div(100)
                val market_pic_qty = Ints.fromByteArray(ca.sliceArray(l1Off + 36 until l1Off + 40)).toLong()
                val market_num_orders = Ints.fromByteArray(ca.sliceArray(l1Off + 40 until l1Off + 44)).toLong()

                if (klines.isNotEmpty()) {
                    klines.find { it.symbol == symbol }?.let { k ->
                        val seconds_nearest_minute = Calendar.getInstance().apply {
                            timeInMillis = LTT
                            set(Calendar.SECOND, 0)
                            set(Calendar.MILLISECOND, 0)
                        }.timeInMillis.div(1000)
                        val i = k.t.indexOfFirst { it == seconds_nearest_minute }
                        if (k.t.getOrNull(i) != null) {
                            if (h > k.h[i])
                                k.h[i] = h.toFloat()
                            else  if (ltp > k.h[i])
                                k.h[i] = ltp.toFloat()

                             if (l < k.l[i])
                                k.l[i] = l.toFloat()
                            else if (ltp < k.l[i])
                                k.l[i] = ltp.toFloat()

                            k.c[i] = ltp.toFloat()
                            k.v[i] = volumeToday.minus(k.v.sum().minus(k.v.last()))

                        } else {
                            k.t.add(seconds_nearest_minute)
                            k.tOG.add(seconds_nearest_minute)
                            k.h.add(h.toFloat())
                            k.o.add(o.toFloat())
                            k.l.add(l.toFloat())
                            k.c.add(ltp.toFloat())
                            k.v.add(volumeToday.minus(k.v.sum()))
                        }

                        k.invalidate.invoke()


                    }


                }

                if (process_MCCSH_UI_LOGIC) {
                    var find = MCCSH.rmcList.find { it.symbol == symbol }
                    if (find == null) {
                        find = ResponseMC(symbol = symbol)
                        MCCSH.rmcList.add(find)
                    }
                    find.let { rmc ->
                        val seconds_nearest_minute = Calendar.getInstance().apply {
                            timeInMillis = LTT
                            set(Calendar.SECOND, 0)
                            set(Calendar.MILLISECOND, 0)
                        }.timeInMillis.div(1000)
                        val i = rmc.t.indexOfFirst { it == seconds_nearest_minute }
                        if (rmc.t.getOrNull(i) != null) {
                            if (h > rmc.h[i])
                                rmc.h[i] = h
                            else if (ltp > rmc.h[i])
                                rmc.h[i] = ltp

                             if (l < rmc.l[i])
                                rmc.l[i] = l
                            else if (ltp < rmc.l[i])
                                rmc.l[i] = ltp

                            rmc.c[i] = ltp
                            rmc.v[i] = volumeToday.minus(rmc.v.sum().minus(rmc.v.last()))

                        } else {
                            rmc.t.add(seconds_nearest_minute)
                            rmc.h.add(h)
                            rmc.o.add(o)
                            rmc.l.add(l)
                            rmc.c.add(ltp)
                            rmc.v.add(volumeToday.minus(rmc.v.sum()))
                        }

                        rmc.calculateVWapHighLowDoblesTan()
                        process_MCCSH_UI_LOGIC_function?.invoke()
                    }

                }


            }


        }


    }


    suspend fun dummy() {
        klines.forEach {
            it.c.clear()
            it.o.clear()
            it.h.clear()
            it.l.clear()
            it.v.clear()
            it.t.clear()
        }
        var list = Series.getMyListScrips("intra")

        var randomInt = Random()
        var randomDouble = Random()
        var randomBool = Random()
        var fakeTime=Calendar.getInstance().apply {
            set(Calendar.DATE,15)
            set(Calendar.HOUR_OF_DAY,9)
            set(Calendar.MINUTE,15)
            set(Calendar.SECOND,0)
            set(Calendar.MILLISECOND,0)

        }.timeInMillis


        while (process_MCCSH_UI_LOGIC) {
            var symbol = list.random()
            var d=randomInt.nextLong().absoluteValue % 100
                fakeTime=fakeTime+d
            delay(5)
            var find = MCCSH.rmcList.find { it.symbol == symbol }
            if (find == null) {
                find = ResponseMC(symbol = symbol)
                MCCSH.rmcList.add(find)

            }
            find.let { rmc ->

                var kline= klines.find { it.symbol==symbol }
                val seconds_nearest_minute = Calendar.getInstance().apply {
                    timeInMillis = fakeTime
                    set(Calendar.SECOND, 0)
                    set(Calendar.MILLISECOND, 0)
                }.timeInMillis.div(1000)
                val i = rmc.t.indexOfFirst { it == seconds_nearest_minute }
                if (rmc.t.getOrNull(i) != null) {

                    var rd = randomDouble.nextDouble() % .3
                    var rb = randomBool.nextBoolean()
                    if (rb) rd=-rd
                    var ltp = addPercentage(rmc.c.last(), rd)



                    if (ltp > rmc.h[i])
                        rmc.h[i] = ltp
                    else if (ltp < rmc.l[i])
                        rmc.l[i] = ltp
                    rmc.c[i] = ltp
                    rmc.v[i] = 0
                    if (kline!=null){
                        var ki=kline!!.c.size-1
                        kline!!.c[ki]=rmc.c.last().toFloat()
                        kline!!.o[ki]=rmc.o.last().toFloat()
                        kline!!.h[ki]=rmc.h.last().toFloat()
                        kline!!.l[ki]=rmc.l.last().toFloat()
                        kline!!.v[ki]=rmc.v.last().toLong()
                        kline!!.t[ki]=rmc.t.last().toLong()
                        kline.invalidate.invoke()
                    }



                } else {

                    if (rmc.c.isEmpty()) {
                        var os = MCCSH.ohlcp.find { it.symbol == symbol }
                        rmc.t.add(seconds_nearest_minute)
                        rmc.h.add(os!!.tOpen)
                        rmc.o.add(os!!.tOpen)
                        rmc.l.add(os!!.tOpen)
                        rmc.c.add(os!!.tOpen)
                        rmc.v.add(0)

                        kline?.c?.add(rmc.c.last().toFloat())
                        kline?.o?.add(rmc.o.last().toFloat())
                        kline?.h?.add(rmc.h.last().toFloat())
                        kline?.l?.add(rmc.l.last().toFloat())
                        kline?.v?.add(rmc.v.last().toLong())
                        kline?.t?.add(rmc.t.last().toLong())
                        kline?.invalidate?.invoke()

                    } else {
                        var rd = randomDouble.nextDouble() % .3
                        var rb = randomBool.nextBoolean()
                        if (rb) rd=-rd
                        var ltp = addPercentage(rmc.c.last(), rd)
                        rmc.t.add(seconds_nearest_minute)
                        rmc.h.add(ltp)
                        rmc.o.add(ltp)
                        rmc.l.add(ltp)
                        rmc.c.add(ltp)
                        rmc.v.add(0)


                        kline?.c?.add(rmc.c.last().toFloat())
                        kline?.o?.add(rmc.o.last().toFloat())
                        kline?.h?.add(rmc.h.last().toFloat())
                        kline?.l?.add(rmc.l.last().toFloat())
                        kline?.v?.add(rmc.v.last().toLong())
                        kline?.t?.add(rmc.t.last().toLong())
                        kline?.invalidate?.invoke()
                    }


                }




                rmc.calculateVWapHighLowDoblesTan()
                process_MCCSH_UI_LOGIC_function?.invoke()
            }

        }

    }

}
