package org.springframework.cloud.rsocket.sample.ping;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.micrometer.MicrometerRSocketInterceptor;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.RSocketProxy;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.gateway.rsocket.support.Metadata;
import org.springframework.context.annotation.Bean;
import org.springframework.core.codec.CharSequenceEncoder;
import org.springframework.core.codec.StringDecoder;
import org.springframework.core.env.Environment;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import org.springframework.messaging.rsocket.MessageHandlerAcceptor;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;

@SpringBootApplication
public class PingApplication {

	@Bean
	public MessageHandlerAcceptor messageHandlerAcceptor() {
		MessageHandlerAcceptor acceptor = new MessageHandlerAcceptor();
		acceptor.setRSocketStrategies(rsocketStrategies());
		return acceptor;
	}

	@Bean
	public RSocketStrategies rsocketStrategies() {
		return RSocketStrategies.builder()
				.decoder(StringDecoder.allMimeTypes())
				.encoder(CharSequenceEncoder.allMimeTypes())
				.dataBufferFactory(new NettyDataBufferFactory(PooledByteBufAllocator.DEFAULT))
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(PingApplication.class, args);
	}

	@Component
	@Slf4j
	public static class Ping implements ApplicationRunner {

		private final Environment env;
		private final MeterRegistry meterRegistry;
		private final RSocketStrategies rSocketStrategies;

		private final String id;

		private final AtomicInteger pongsReceived = new AtomicInteger();

		private final AtomicBoolean running = new AtomicBoolean();

		private RSocket rSocket;

		public Ping(Environment env, MeterRegistry meterRegistry, RSocketStrategies rSocketStrategies) {
			this.id = env.getProperty("ping.id", "1");
			this.env = env;
			this.meterRegistry = meterRegistry;
			this.rSocketStrategies = rSocketStrategies;
		}

		@Override
		public void run(ApplicationArguments args) throws Exception {

			if (this.running.compareAndSet(false, true)) {

				String requestType = env
						.getProperty("ping.request-type", "request-channel");
				log.info("Starting Ping" + id + " request type: " + requestType);

				Integer serverPort = env
						.getProperty("spring.cloud.gateway.rsocket.server.port",
								Integer.class, 7002);

				MicrometerRSocketInterceptor interceptor = new MicrometerRSocketInterceptor(meterRegistry, Tag
						.of("component", "ping"));
				ByteBuf announcementMetadata = Metadata.from("ping")
						.with("id", "ping" + id).encode();

				Function<RSocketRequester, Publisher<String>> handler;
				if (requestType.equals("request-response")) {
					handler = this::handleRequestResponse;
				} else {
					handler = this::handleRequestChannel;
				}

				RSocketFactory.connect()
						.metadataMimeType(Metadata.ROUTING_MIME_TYPE)
						.setupPayload(DefaultPayload
								.create(EMPTY_BUFFER, announcementMetadata))
						.addClientPlugin(interceptor)
						.addClientPlugin(StringToMetadataRSocket::new)
						.transport(TcpClientTransport.create(serverPort))
						.start()
						.map(socket -> {
							this.rSocket = socket;
							return RSocketRequester.create(this.rSocket, MimeTypeUtils.TEXT_PLAIN, this.rSocketStrategies);
						})
						.flatMapMany(handler)
						.subscribe();

			}
		}

		private Flux<String> handleRequestResponse(RSocketRequester requester) {
			return Flux.interval(Duration.ofSeconds(1))
					.flatMap(i -> requester.route("pong")
							.data(getPayload(i))
							.retrieveMono(String.class)
							.doOnNext(this::logPongs));
		}

		private Flux<String> handleRequestChannel(RSocketRequester requester) {
			return requester.route("pong")
					.data(sendPings(), String.class)
					.retrieveFlux(String.class)
					.doOnNext(this::logPongs);
		}

		AtomicInteger dropped = new AtomicInteger();

		private Flux<String> sendPings() {
			return Flux.interval(Duration.ofSeconds(1))
					.map(this::getPayload)
					.log("send-pings")
					// this is needed in case pong is not available yet
					.onBackpressureDrop(payload -> log.info("Dropped payload ("+
							this.dropped.incrementAndGet()+") " + payload));
		}

		private String getPayload(long i) {
			return "ping" + this.id+"-"+i;
		}

		private void logPongs(String payload) {
			int received = this.pongsReceived.incrementAndGet();
			log.info("received " + payload + "(" + received + ") in Ping" + this.id);
		}
	}

	private static class StringToMetadataRSocket extends RSocketProxy {

		public StringToMetadataRSocket(RSocket source) {
			super(source);
		}

		@Override
		public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
			Flux<Payload> converted = Flux.from(payloads)
					.map(payload -> {
						if (!payload.hasMetadata()) {
							return payload;
						}
						String name = payload.getMetadataUtf8();
						ByteBuf metadata = Metadata.from(name).encode();
						return DefaultPayload.create(payload.sliceData(), metadata);
					}).log("string-to-metadata-request-channel");
			return super.requestChannel(converted);
		}
	}
}

