package zio.memberlist

import zio.config.ConfigDescriptor._
import zio.config.{ConfigDescriptor, ReadError, ZConfig}
import zio.duration.{Duration, _}
import zio.memberlist.state.NodeName
import zio.{Chunk, Has, ZLayer}

import java.net.InetAddress

case class MemberlistConfig(
  name: NodeName,
  bindAddress: NodeAddress,
  protocolInterval: Duration,
  protocolTimeout: Duration,
  messageSizeLimit: Int,
  broadcastResent: Int,
  localHealthMaxMultiplier: Int,
  suspicionAlpha: Int,
  suspicionBeta: Int,
  suspicionRequiredConfirmations: Int
)

object MemberlistConfig {

  val description: ConfigDescriptor[MemberlistConfig] =
    (string("NAME").apply[NodeName](NodeName(_), nn => Some(nn.name)) |@|
      string("ADDRESS")
        .default("0.0.0.0")
        .zip(int("PORT").default(5557))[NodeAddress](
          { case (addr, port) => NodeAddress(Chunk.fromArray(InetAddress.getByName(addr).getAddress), port) },
          address => Some((InetAddress.getByAddress(address.addr.toArray).getHostName, address.port))
        ) |@|
      zioDuration("PROTOCOL_INTERVAL").default(1.second) |@|
      zioDuration("PROTOCOL_TIMEOUT").default(500.milliseconds) |@|
      int("MESSAGE_SIZE_LIMIT").default(64000) |@|
      int("BROADCAST_RESENT").default(10) |@|
      int("LOCAL_HEALTH_MAX_MULTIPLIER").default(8) |@|
      int("SUSPICION_ALPHA_MULTIPLIER").default(9) |@|
      int("SUSPICION_BETA_MULTIPLIER").default(9) |@|
      int("SUSPICION_CONFIRMATIONS")
        .default(3)).to[MemberlistConfig]

  val fromEnv: ZLayer[zio.system.System, ReadError[String], Has[MemberlistConfig]] =
    ZConfig.fromSystemEnv(description)
}
