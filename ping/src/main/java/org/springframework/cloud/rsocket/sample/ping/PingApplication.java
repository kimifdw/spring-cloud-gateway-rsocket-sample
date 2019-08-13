package org.springframework.cloud.rsocket.sample.ping;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.micrometer.MicrometerRSocketInterceptor;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.gateway.rsocket.support.Metadata;
import org.springframework.cloud.gateway.rsocket.support.RouteSetup;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.messaging.rsocket.MetadataExtractor;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;

@SpringBootApplication
public class PingApplication {

	public static void main(String[] args) {
		SpringApplication.run(PingApplication.class, args);
	}

	@Component
	@Slf4j
	public static class Ping implements ApplicationListener<ApplicationReadyEvent> {

		private MeterRegistry meterRegistry;
		private final RSocketRequester.Builder requesterBuilder;

		private final String id;

		private final AtomicInteger pongsReceived = new AtomicInteger();
		private Flux<String> pongFlux;

		public Ping(Environment env, MeterRegistry meterRegistry, RSocketRequester.Builder requesterBuilder) {
			this.id = env.getProperty("ping.id", "1");
			this.meterRegistry = meterRegistry;
			this.requesterBuilder = requesterBuilder;
		}

		@Override
		public void onApplicationEvent(ApplicationReadyEvent event) {
			ConfigurableEnvironment env = event.getApplicationContext().getEnvironment();

			String requestType = env.getProperty("ping.request-type", "request-channel");
			log.info("Starting Ping"+id+" request type: " + requestType);

			Integer serverPort = env.getProperty("spring.rsocket.server.port",
					Integer.class, 7002);

			MicrometerRSocketInterceptor interceptor = new MicrometerRSocketInterceptor(meterRegistry, Tag
					.of("component", "ping"));
			//ByteBuf announcementMetadata = Metadata.from("ping").with("id", "ping"+id).encode();

			/*Function<RSocket, Publisher<String>> handler;
			if (requestType.equals("request-response")) {
				handler = this::handleRequestResponse;
			} else {
				handler = this::handleRequestChannel;
			}*/

			RSocketRequester requester = requesterBuilder
					.connectTcp("localhost", serverPort).block();

			RouteSetup metadata = new RouteSetup(Metadata.from("ping")
					.with("id", "ping" + id).build());

			Flux.interval(Duration.ofSeconds(1))
					.flatMap(i -> requester.metadata(metadata, RouteSetup.ROUTE_SETUP_MIME_TYPE)
							.data("ping" + i)
							.retrieveMono(String.class)
							.doOnNext(this::logPongs))
					.subscribe();

			/*pongFlux = RSocketFactory.connect()
					.metadataMimeType(MetadataExtractor.COMPOSITE_METADATA)
					.setupPayload(DefaultPayload
							.create(EMPTY_BUFFER, announcementMetadata))
					.addClientPlugin(interceptor)
					.transport(TcpClientTransport.create(serverPort))
					.start()
					.flatMapMany(handler);

			pongFlux.subscribe();*/
		}

		/*private Flux<String> handleRequestResponse(RSocket rSocket) {
			return Flux.interval(Duration.ofSeconds(1))
					.flatMap(i -> rSocket.requestResponse(getPayload(i))
							.map(Payload::getDataUtf8)
							.doOnNext(this::logPongs));
		}

		private Flux<String> handleRequestChannel(RSocket socket) {
			return socket.requestChannel(sendPings()
					// this is needed in case pong is not available yet
					.onBackpressureDrop(payload -> log.info("Backpressure applied, dropping payload " + payload.getDataUtf8()))
			).map(Payload::getDataUtf8)
					.doOnNext(this::logPongs);
		}

		private Flux<Payload> sendPings() {
			return Flux.interval(Duration.ofSeconds(1))
					.map(this::getPayload);
		}

		private Payload getPayload(long i) {
			ByteBuf data = ByteBufUtil
					.writeUtf8(ByteBufAllocator.DEFAULT, "ping" + id);
			ByteBuf routingMetadata = Metadata.from("pong").encode();
			return DefaultPayload.create(data, routingMetadata);
		}*/

		private void logPongs(String payload) {
			int received = pongsReceived.incrementAndGet();
			log.info("received " + payload + "(" + received + ") in Ping" + id);
		}
	}
}

