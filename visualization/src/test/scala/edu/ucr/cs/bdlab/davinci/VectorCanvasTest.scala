package edu.ucr.cs.bdlab.davinci

import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, GeometryReader}
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{CoordinateXY, Envelope}
import org.locationtech.jts.io.WKTReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.collection.mutable
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class VectorCanvasTest extends AnyFunSuite with ScalaSparkTest {

  test("add points to vector canvas") {
    val canvas = new VectorCanvas(new Envelope(0, 256, 0, 256),
      256, 256, 0, 1)
    val factory = GeometryReader.DefaultGeometryFactory
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(5, 5)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(4.9, 4.9)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0.1, 0.1)), null)
    assert(canvas.geometries.length == 2)
  }

  test("skip empty linestrings") {
    val canvas = new VectorCanvas(new Envelope(0, 256, 0, 256),
      256, 256, 0, 1)
    val factory = GeometryReader.DefaultGeometryFactory
    canvas.rasterizeGeometry(factory.createGeometryCollection(Array(factory.createLineString(),
      factory.createPoint(new CoordinateXY(0, 0)))))
    assert(canvas.occupiedPixels.countOnes() == 1)
  }

  test("should rasterize too many points") {
    val canvas = new VectorCanvas(new Envelope(0, 256, 0, 256),
      256, 256, 0, 1)
    val factory = GeometryReader.DefaultGeometryFactory
    for (x <- 0 to 255; y <- 0 to 255)
      canvas.addGeometry(factory.createPoint(new CoordinateXY(x, y)), null)
    assert(canvas.geometries.isEmpty)
  }

  test("add points at the top edge") {
    val canvas = new VectorCanvas(new Envelope(0, 256, 0, 256),
      256, 256, 0, 1)
    val factory = GeometryReader.DefaultGeometryFactory
    canvas.addGeometry(factory.createPoint(new CoordinateXY(256.0, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0.0, 255.9)), null)
    assert(canvas.geometries.length == 2)
  }

  test("add points in the buffer area") {
    val canvas = new VectorCanvas(new Envelope(0, 256, 0, 256),
      256, 256, 0, 5)
    val factory = GeometryReader.DefaultGeometryFactory
    canvas.addGeometry(factory.createPoint(new CoordinateXY(-2, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(256+4, 255.9)), null)
    assert(canvas.geometries.length == 2)
  }

  test("merge canvases") {
    val canvas1 = new VectorCanvas(new Envelope(0, 256, 0, 256),
      256, 256, 0, 1)
    val canvas2 = new VectorCanvas(new Envelope(0, 256, 0, 256),
      256, 256, 0, 1)
    val factory = GeometryReader.DefaultGeometryFactory
    canvas1.addGeometry(factory.createPoint(new CoordinateXY(0, 0)), null)
    canvas1.addGeometry(factory.createPoint(new CoordinateXY(5, 5)), null)
    canvas2.addGeometry(factory.createPoint(new CoordinateXY(4.9, 4.9)), null)
    canvas2.addGeometry(factory.createPoint(new CoordinateXY(0.1, 0.1)), null)
    canvas1.mergeWith(canvas2)
    assert(canvas1.geometries.length == 2)
  }

  test("merge canvases should not convert geometries twice") {
    val canvas1 = new VectorCanvas(new Envelope(0, 1, 0, 1),
      10, 10, 0, 1)
    val canvas2 = new VectorCanvas(new Envelope(0, 1, 0, 1),
      10, 10, 0, 1)
    val factory = GeometryReader.DefaultGeometryFactory
    canvas1.addGeometry(factory.createPoint(new CoordinateXY(0, 0)), null)
    canvas1.addGeometry(factory.createPoint(new CoordinateXY(0.1, 0.1)), null)
    canvas2.addGeometry(factory.createPoint(new CoordinateXY(0.2, 0.2)), null)
    canvas2.addGeometry(factory.createPoint(new CoordinateXY(0.3, 0.3)), null)
    canvas1.mergeWith(canvas2)
    assert(canvas1.geometries.length == 4)
    assert(canvas1.geometries(2)._1.getCoordinate.x == 2.0)
    assert(canvas1.geometries(3)._1.getCoordinate.x == 3.0)
  }

  test("Create blocked regions simple") {
    val canvas = new VectorCanvas(new Envelope(0, 3, 0, 3),
      3, 3, 0, 5)
    val factory = GeometryReader.DefaultGeometryFactory
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 1)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(1, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(1, 1)), null)
    canvas.geometries.clear()
    val rings = canvas.createRingsForOccupiedPixels
    assert(rings.length == 1)
  }

  test("Create blocked region in the buffer area") {
    val canvas = new VectorCanvas(new Envelope(0, 3, 0, 3),
      3, 3, 0, 2)
    val factory = GeometryReader.DefaultGeometryFactory
    canvas.rasterizeGeometry(new EnvelopeND(factory, 2, -2, -2, 1, 1))
    val ring = canvas.createRingsForOccupiedPixels.head
    val ringMBR = ring.getEnvelopeInternal
    assert(ringMBR.getArea == 9.0)
  }

  test("Create blocked regions complex") {
    val canvas = new VectorCanvas(new Envelope(0, 5, 0, 4),
      5, 4, 0, 0)
    val factory = GeometryReader.DefaultGeometryFactory
    // First block
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 1)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(1, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(1, 1)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(2, 1)), null)
    // Second block
    canvas.addGeometry(factory.createPoint(new CoordinateXY(3, 2)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(4, 2)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(3, 3)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(4, 3)), null)
    val rings = canvas.createRingsForOccupiedPixels
    assert(rings.length == 2)
  }

  test("Create blocked regions with a hole") {
    val canvas = new VectorCanvas(new Envelope(0, 4, 0, 5),
      4, 5, 0, 0)
    val factory = GeometryReader.DefaultGeometryFactory
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(1, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(2, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(3, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(3, 1)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(3, 2)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(3, 3)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(2, 3)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(2, 4)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(1, 4)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 4)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 3)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 2)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 1)), null)
    val rings = canvas.createRingsForOccupiedPixels
    assert(rings.length == 2)
  }

  test("Create blocked region with a hole and block inside") {
    val canvas = new VectorCanvas(new Envelope(0, 6, 0, 6),
      6, 6, 0, 0)
    val factory = GeometryReader.DefaultGeometryFactory
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(1, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(2, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(3, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(4, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(5, 0)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(5, 1)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(5, 2)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(5, 3)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(5, 4)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(5, 5)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(4, 5)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(3, 5)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(2, 5)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(1, 5)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 5)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 4)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 3)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 2)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(0, 1)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(2, 2)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(2, 3)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(3, 3)), null)
    canvas.addGeometry(factory.createPoint(new CoordinateXY(3, 2)), null)
    val rings = canvas.createRingsForOccupiedPixels
    assert(rings.length == 3)
  }

  test("Work with non point geometries") {
    val canvas = new VectorCanvas(new Envelope(0, 6, 0, 6),
      6, 6, 0, 0)
    val factory = GeometryReader.DefaultGeometryFactory
    canvas.addGeometry(factory.toGeometry(new Envelope(0,3,0,3)), null)
    assert(canvas.geometries.length == 1)
  }

  test("Find line intersections") {
    val canvas = new VectorCanvas(new Envelope(0, 5, 0, 5),
      5, 5, 0, 5)
    val intersections = new mutable.ArrayBuffer[(Int, Int)]()
    canvas.findIntersections(0, 0, 3, 4, intersections)
    assert(intersections.length == 4)
    assert(intersections.contains((0, 0)))
    assert(intersections.contains((1, 1)))
    assert(intersections.contains((1, 2)))
    assert(intersections.contains((2, 3)))
  }

  test("Rasterize geometry near the boundaries") {
    val canvas = new VectorCanvas(new Envelope(0, 0, 256, 256),
      256, 256, 0, 4)
    val geometry = new WKTReader().read("POLYGON ((260 218.4302106226665, 215.73763982222255 218.42054586199222, 215.73746232906663 260, 260 260, 260 218.4302106226665))")
    canvas.rasterizeGeometry(geometry)
    assert(canvas.occupiedPixels.countOnes() > 0)
  }

  test("Special case with generating edges") {
    // This case is hard to handcraft as it depends on the order of edges in the hashmap
    // This depends on the hash function used with tuples and the hashmap size which are hard to adjust
    // Therefore, we try the following case which happened with real data and resulted in the corner case we check
    // The case is when two regions meet in a corner point. This results in two points with the same coordinate
    // The internal map should still keep them separate even though they are equal with the "==" notion.
    val bitArray = new edu.ucr.cs.bdlab.beast.util.BitArray(69696);
    bitArray.entries(0) = -9223372036854775808L
    bitArray.entries(2) = 67108864L
    bitArray.entries(3) = 17592186044416L
    bitArray.entries(4) = 7864320L
    bitArray.entries(6) = 34359738368L
    bitArray.entries(7) = -1125899906842624L
    bitArray.entries(8) = 2013331455L
    bitArray.entries(10) = 17592186044416L
    bitArray.entries(11) = -72057594037927936L
    bitArray.entries(12) = 257714814975L
    bitArray.entries(14) = 9007199254740992L
    bitArray.entries(15) = -9223372036854775808L
    bitArray.entries(16) = 39583492341759L
    bitArray.entries(18) = 4611686018427387904L
    bitArray.entries(20) = 8444309430861944L
    bitArray.entries(23) = 128;
    bitArray.entries(24) = 2161735517719232000L
    bitArray.entries(27) = 65536;
    bitArray.entries(28) = 2251799813652480L
    bitArray.entries(29) = 15;
    bitArray.entries(31) = 33554432L
    bitArray.entries(32) = 574208952487641088L
    bitArray.entries(33) = 3968;
    bitArray.entries(35) = 17179869184L
    bitArray.entries(36) = -33554432L
    bitArray.entries(37) = 491751L
    bitArray.entries(39) = 4398046511104L
    bitArray.entries(40) = -1073741824L
    bitArray.entries(41) = 62928895L
    bitArray.entries(43) = 2251799813685248L
    bitArray.entries(44) = -34359738368L
    bitArray.entries(45) = 8054636543L
    bitArray.entries(47) = 576460752303423488L
    bitArray.entries(48) = -2199023255552L
    bitArray.entries(49) = 2130706417663L
    bitArray.entries(52) = -70368744177616L
    bitArray.entries(53) = 263951509684223L
    bitArray.entries(56) = -4503599627354112L
    bitArray.entries(57) = 31533993472032767L
    bitArray.entries(60) = -288230376143323136L
    bitArray.entries(61) = 4037477065132343295L
    bitArray.entries(64) = -9223372034707292160L
    bitArray.entries(65) = 576460692173881343L
    bitArray.entries(66) = 30;
    bitArray.entries(68) = 1099511627776L
    bitArray.entries(69) = -1649267441696L
    bitArray.entries(70) = 7943;
    bitArray.entries(72) = 281474976710656L
    bitArray.entries(73) = -105553116267520L
    bitArray.entries(74) = 985087L
    bitArray.entries(76) = 72057594037927936L
    bitArray.entries(77) = -6755399441121280L
    bitArray.entries(78) = 252182527L
    bitArray.entries(81) = -4194303L
    bitArray.entries(82) = 13019119615L
    bitArray.entries(85) = -16776704L
    bitArray.entries(86) = 34359738311L
    bitArray.entries(89) = -1073610752L
    bitArray.entries(90) = 8796093006815L
    bitArray.entries(93) = -274844352512L
    bitArray.entries(94) = 2251799813682175L
    bitArray.entries(97) = -35167192219648L
    bitArray.entries(98) = 576460751281913855L
    bitArray.entries(101) = -4499201580859392L
    bitArray.entries(102) = -260856348673L
    bitArray.entries(103) = 7;
    bitArray.entries(105) = -1151795604700004352L
    bitArray.entries(106) = -67343476588545L
    bitArray.entries(107) = 2047;
    bitArray.entries(109) = 288230376151711744L
    bitArray.entries(110) = -2041243336966152L
    bitArray.entries(111) = 524287L
    bitArray.entries(114) = -2643617379312994296L
    bitArray.entries(115) = 134217727L
    bitArray.entries(118) = -2163979620951783424L
    bitArray.entries(119) = 34359738329L
    bitArray.entries(122) = -108086391089922048L
    bitArray.entries(123) = 8796093005903L
    bitArray.entries(126) = 4611686009971671040L
    bitArray.entries(127) = 2251799813296113L
    bitArray.entries(130) = -1082331758592L
    bitArray.entries(131) = 576460752030923647L
    bitArray.entries(134) = -277076930199552L
    bitArray.entries(135) = -241172578305L
    bitArray.entries(136) = 7;
    bitArray.entries(138) = -34902897112121344L
    bitArray.entries(139) = -35604205142017L
    bitArray.entries(140) = 2047;
    bitArray.entries(142) = -4323455642275676160L
    bitArray.entries(143) = -10968831077908481L
    bitArray.entries(144) = 524287L
    bitArray.entries(147) = -5473009892522459198L
    bitArray.entries(148) = 134217727L
    bitArray.entries(151) = 8104790479406622208L
    bitArray.entries(152) = 34359735491L
    bitArray.entries(155) = 6809442499145105408L
    bitArray.entries(156) = 8796092086272L
    bitArray.entries(159) = 9223372036821221376L
    bitArray.entries(160) = 2251799813462393L
    bitArray.entries(163) = -1196277240954880L
    bitArray.entries(164) = 576460752250009727L
    bitArray.entries(167) = -19141397927952384L
    bitArray.entries(168) = 1152920374768271359L
    bitArray.entries(171) = -90353467524120576L
    bitArray.entries(172) = -289188477927425L
    bitArray.entries(173) = 65287L
    bitArray.entries(175) = -4107282860161892352L
    bitArray.entries(176) = -146252761086820353L
    bitArray.entries(177) = 16775679L
    bitArray.entries(180) = -5;
    bitArray.entries(181) = 4294967295L
    bitArray.entries(184) = -128;
    bitArray.entries(185) = 1099511627775L
    bitArray.entries(188) = -32768L
    bitArray.entries(189) = 281474976710655L
    bitArray.entries(192) = -8388608L
    bitArray.entries(193) = 72057594037927935L
    bitArray.entries(196) = -2147483648L
    bitArray.entries(197) = -917505L
    bitArray.entries(200) = -274877906944L
    bitArray.entries(201) = -2143289345L
    bitArray.entries(202) = 255;
    bitArray.entries(204) = -70368744177664L
    bitArray.entries(205) = -2198754820097L
    bitArray.entries(206) = 65535L
    bitArray.entries(208) = -18014398509481984L
    bitArray.entries(209) = -1125882726973441L
    bitArray.entries(210) = 16777215L
    bitArray.entries(212) = 6917529027641081856L
    bitArray.entries(213) = -288229276640083972L
    bitArray.entries(214) = 4294967295L
    bitArray.entries(217) = 35184372080752L
    bitArray.entries(218) = 1099511627772L
    bitArray.entries(220) = -1152921504606846976L
    bitArray.entries(221) = 35184237883393L
    bitArray.entries(222) = 281474976708608L
    bitArray.entries(225) = 1839000L
    bitArray.entries(226) = 72057594037403648L
    bitArray.entries(229) = 134088704L
    bitArray.entries(230) = -134217728L
    bitArray.entries(233) = 8053587968L
    bitArray.entries(234) = -34359738368L
    bitArray.entries(235) = 255;
    bitArray.entries(237) = 402653184L
    bitArray.entries(238) = -8796093022208L
    bitArray.entries(239) = 65535L
    bitArray.entries(241) = 206158430208L
    bitArray.entries(242) = -2251799813685248L
    bitArray.entries(243) = 16777215L
    bitArray.entries(245) = 123145302310912L
    bitArray.entries(246) = -576460752303423488L
    bitArray.entries(247) = 4294967295L
    bitArray.entries(249) = 63050394783186944L
    bitArray.entries(251) = 1099511627768L
    bitArray.entries(253) = -4611686018427387904L
    bitArray.entries(254) = 1;
    bitArray.entries(255) = 281474976708608L
    bitArray.entries(258) = 896;
    bitArray.entries(259) = 72057594037403648L
    bitArray.entries(262) = 393216L
    bitArray.entries(263) = -134217728L
    bitArray.entries(266) = 201326592L
    bitArray.entries(267) = -34359738368L
    bitArray.entries(268) = 255;
    bitArray.entries(270) = 103079215104L
    bitArray.entries(271) = -8796093022208L
    bitArray.entries(272) = 65535L
    bitArray.entries(274) = 26388279066624L
    bitArray.entries(275) = -2251799813685248L
    bitArray.entries(276) = 16777215L
    bitArray.entries(278) = 15762598695796736L
    bitArray.entries(279) = -288230376151711744L
    bitArray.entries(280) = 4294967295L
    bitArray.entries(282) = 4035225266123964416L
    bitArray.entries(284) = 1099511627772L
    bitArray.entries(287) = 48;
    bitArray.entries(288) = 281474976709632L
    bitArray.entries(291) = 12288L
    bitArray.entries(292) = 72057594037796864L
    bitArray.entries(295) = 3145728L
    bitArray.entries(296) = -33554432L
    bitArray.entries(299) = 805306368L
    bitArray.entries(300) = -8589934592L
    bitArray.entries(301) = 255;
    bitArray.entries(303) = 240518168576L
    bitArray.entries(304) = -1099511627776L
    bitArray.entries(305) = 65535L
    bitArray.entries(307) = 26388279066624L
    bitArray.entries(308) = -281474976710656L
    bitArray.entries(309) = 16777215L
    bitArray.entries(311) = 6755399441055744L
    bitArray.entries(312) = -36028797018963968L
    bitArray.entries(313) = 4294967295L
    bitArray.entries(315) = 1729382256910270464L
    bitArray.entries(316) = -9223372036854775808L
    bitArray.entries(317) = 1099511627775L
    bitArray.entries(320) = 12;
    bitArray.entries(321) = 281474976710592L
    bitArray.entries(324) = 3072;
    bitArray.entries(325) = 72057594037911552L
    bitArray.entries(328) = 786432L
    bitArray.entries(329) = -4194304L
    bitArray.entries(332) = 201326592L
    bitArray.entries(333) = -1073741824L
    bitArray.entries(334) = 255;
    bitArray.entries(336) = 25769803776L
    bitArray.entries(337) = -274877906944L
    bitArray.entries(338) = 65535L
    bitArray.entries(340) = 6597069766656L
    bitArray.entries(341) = -70368744177664L
    bitArray.entries(342) = 16777215L
    bitArray.entries(344) = 1688849860263936L
    bitArray.entries(345) = -18014398509481984L
    bitArray.entries(346) = 4294967295L
    bitArray.entries(348) = 216172782113783808L
    bitArray.entries(349) = -4611686018427387904L
    bitArray.entries(350) = 1099511627775L
    bitArray.entries(353) = 3;
    bitArray.entries(354) = 281474976710592L
    bitArray.entries(357) = 896;
    bitArray.entries(358) = 72057594037911552L
    bitArray.entries(361) = 98304L
    bitArray.entries(362) = -4194304L
    bitArray.entries(365) = 29360128L
    bitArray.entries(366) = -1073741824L
    bitArray.entries(367) = 255;
    bitArray.entries(369) = 3221225472L
    bitArray.entries(370) = -274877906944L
    bitArray.entries(371) = 65535L
    bitArray.entries(373) = 824633720832L
    bitArray.entries(374) = -70368744177664L
    bitArray.entries(375) = 16777215L
    bitArray.entries(377) = 246290604621824L
    bitArray.entries(378) = -18014398509481984L
    bitArray.entries(379) = 4294967295L
    bitArray.entries(381) = 27021597764222976L
    bitArray.entries(382) = -9223372036854775808L
    bitArray.entries(383) = 1099511615487L
    bitArray.entries(385) = 8070450532247928832L
    bitArray.entries(387) = 281474968583936L
    bitArray.entries(390) = 48;
    bitArray.entries(391) = 72057591923933184L
    bitArray.entries(394) = 14336L
    bitArray.entries(395) = -549755813888L
    bitArray.entries(398) = 1572864L
    bitArray.entries(399) = -281474976710656L
    bitArray.entries(400) = 255;
    bitArray.entries(402) = 402653184L
    bitArray.entries(403) = -72057594037927936L
    bitArray.entries(404) = 65535L
    bitArray.entries(406) = 34359738368L
    bitArray.entries(408) = 16777215L
    bitArray.entries(410) = 8796093022208L
    bitArray.entries(412) = 4294967040L
    bitArray.entries(414) = 3377699720527872L
    bitArray.entries(416) = 1099511562240L
    bitArray.entries(418) = 864691128455135232L
    bitArray.entries(420) = 281474959933440L
    bitArray.entries(423) = 56;
    bitArray.entries(424) = 72057589742960640L
    bitArray.entries(427) = 67100672L
    bitArray.entries(428) = -1099511627776L
    bitArray.entries(431) = 1117103813820416L
    bitArray.entries(432) = -281474976710656L
    bitArray.entries(433) = 255;
    bitArray.entries(435) = 2017612633061982208L
    bitArray.entries(436) = -72057594037927936L
    bitArray.entries(437) = 65535L
    bitArray.entries(440) = 112;
    bitArray.entries(441) = 16777215L
    bitArray.entries(444) = 98304L
    bitArray.entries(445) = 4294967040L
    bitArray.entries(448) = 234881024L
    bitArray.entries(449) = 1099511562240L
    bitArray.entries(452) = 103079215104L
    bitArray.entries(453) = 281474968322048L
    bitArray.entries(456) = 52776558133248L
    bitArray.entries(457) = 72057591890444288L
    bitArray.entries(460) = 18014398509481984L
    bitArray.entries(461) = -549755813888L
    bitArray.entries(464) = -4611686018427387904L
    bitArray.entries(465) = -70368744177664L
    bitArray.entries(466) = 255;
    bitArray.entries(469) = -9007199254740864L
    bitArray.entries(470) = 65533L
    bitArray.entries(473) = -1152921504606814208L
    bitArray.entries(474) = 16776703L
    bitArray.entries(477) = 8388608L
    bitArray.entries(478) = 4294770680L
    bitArray.entries(481) = 3221225472L
    bitArray.entries(482) = 1099452905472L
    bitArray.entries(485) = 824633720832L
    bitArray.entries(486) = 281458870321152L
    bitArray.entries(489) = 105553116266496L
    bitArray.entries(490) = 72055669758361600L
    bitArray.entries(493) = 27021597764222976L
    bitArray.entries(494) = -545392127115264L
    bitArray.entries(497) = 3458764513820540928L
    bitArray.entries(498) = -143006880355057664L
    bitArray.entries(499) = 255;
    bitArray.entries(502) = 135107988821114928L
    bitArray.entries(503) = 65534L
    bitArray.entries(506) = -4611686018427373568L
    bitArray.entries(507) = 16776704L
    bitArray.entries(510) = 7208960L
    bitArray.entries(511) = 4294836416L
    bitArray.entries(514) = 3271557120L
    bitArray.entries(515) = 1099478122496L
    bitArray.entries(518) = 1656783634432L
    bitArray.entries(519) = 281466390970368L
    bitArray.entries(522) = 900157387837538304L
    bitArray.entries(523) = 72055395417325568L
    bitArray.entries(526) = 26388279066624L
    bitArray.entries(527) = -1125887021940688L
    bitArray.entries(530) = 3377699720527872L
    bitArray.entries(531) = -144114363442126848L
    bitArray.entries(532) = 255;
    bitArray.entries(534) = 864691128455135232L
    bitArray.entries(535) = 2305948562334154752L
    bitArray.entries(536) = 65535L
    bitArray.entries(539) = 13510799955853318L
    bitArray.entries(540) = 16777088L
    bitArray.entries(543) = 1729383081543991808L
    bitArray.entries(544) = 4294950912L
    bitArray.entries(547) = 211106232729600L
    bitArray.entries(548) = 1099507433484L
    bitArray.entries(551) = 54043195553611776L
    bitArray.entries(552) = 281474439841790L
    bitArray.entries(555) = -4611686011984936960L
    bitArray.entries(556) = 72057456598974592L
    bitArray.entries(559) = 824633720832L
    bitArray.entries(560) = -17592186027968L
    bitArray.entries(563) = 70368744177664L
    bitArray.entries(564) = -2251799807369216L
    bitArray.entries(565) = 255;
    bitArray.entries(567) = 9007199254740992L
    bitArray.entries(568) = -144115187265568768L
    bitArray.entries(569) = 65535L
    bitArray.entries(571) = 1764285154022391808L
    bitArray.entries(572) = 71940702208L
    bitArray.entries(573) = 16777214L
    bitArray.entries(575) = -8791026472627208192L
    bitArray.entries(576) = 16492674416647L
    bitArray.entries(577) = 4294967040L
    bitArray.entries(580) = 1;
    bitArray.entries(581) = 1099511562240L
    bitArray.entries(584) = 384;
    bitArray.entries(585) = 281474968322048L
    bitArray.entries(588) = 98304L
    bitArray.entries(589) = 72057592964186112L
    bitArray.entries(592) = 12582912L
    bitArray.entries(593) = -137438953472L
    bitArray.entries(596) = 3221225472L
    bitArray.entries(597) = -35184372088832L
    bitArray.entries(598) = 255;
    bitArray.entries(600) = 824633720832L
    bitArray.entries(601) = -18014398509481984L
    bitArray.entries(602) = 65535L
    bitArray.entries(604) = 105553116266496L
    bitArray.entries(605) = -4611686018427387904L
    bitArray.entries(606) = 16777215L
    bitArray.entries(608) = 27021597764222976L
    bitArray.entries(610) = 4294967168L
    bitArray.entries(612) = 1170654428139618304L
    bitArray.entries(614) = 1099511595008L
    bitArray.entries(616) = -4577909021222109184L
    bitArray.entries(617) = 15;
    bitArray.entries(618) = 281474968322048L
    bitArray.entries(620) = 432345564227567616L
    bitArray.entries(622) = 72040000778141696L
    bitArray.entries(625) = 2;
    bitArray.entries(626) = 8646981378417623040L
    bitArray.entries(629) = 512;
    bitArray.entries(630) = 17944029765304320L
    bitArray.entries(631) = 120;
    bitArray.entries(633) = 131072L
    bitArray.entries(634) = 4539628424389459968L
    bitArray.entries(635) = 12288L
    bitArray.entries(637) = 100663296L
    bitArray.entries(639) = 56;
    bitArray.entries(641) = 25769803776L
    bitArray.entries(643) = 4096;
    bitArray.entries(645) = 4398046511104L
    bitArray.entries(649) = 2251799813685248L
    bitArray.entries(653) = 576460752303423488L
    bitArray.entries(658) = 16;
    bitArray.entries(662) = 12288L
    bitArray.entries(666) = 2097152L
    bitArray.entries(668) = 480;
    bitArray.entries(670) = 1610612736L
    bitArray.entries(672) = 122880L
    bitArray.entries(674) = 824633720832L
    bitArray.entries(675) = 126100789566373888L
    bitArray.entries(676) = 3072;
    bitArray.entries(678) = 422212465065984L
    bitArray.entries(679) = -9223372036854775808L
    bitArray.entries(680) = 8053063681L
    bitArray.entries(682) = 432345564227567616L
    bitArray.entries(683) = 2;
    bitArray.entries(684) = 1924145348736L
    bitArray.entries(687) = 240;
    bitArray.entries(688) = 422212465098752L
    bitArray.entries(691) = 49152L
    bitArray.entries(692) = 72057594037927936L
    bitArray.entries(695) = 14680064L
    bitArray.entries(699) = 1610612736L
    bitArray.entries(703) = 137438953472L
    bitArray.entries(705) = 128;
    bitArray.entries(707) = 17592186044416L
    bitArray.entries(709) = 98304L
    bitArray.entries(711) = 4503599627370496L
    bitArray.entries(713) = 58720256L
    bitArray.entries(715) = 1152921538966585344L
    bitArray.entries(717) = 30064771072L
    bitArray.entries(719) = 144080003703767040L
    bitArray.entries(720) = 48;
    bitArray.entries(724) = 8195;
    bitArray.entries(728) = 6291968L
    bitArray.entries(732) = 1611005952L
    bitArray.entries(736) = 419833053184L
    bitArray.entries(740) = 15668040695808L
    bitArray.entries(744) = 70368744177664L
    bitArray.entries(748) = 18014398509481984L
    bitArray.entries(752) = 2810246167479189504L
    bitArray.entries(754) = 13510800761159680L
    bitArray.entries(756) = -9223372036854775808L
    bitArray.entries(761) = 64;
    bitArray.entries(762) = 576460752303423488L
    bitArray.entries(765) = 16384L
    bitArray.entries(767) = 12;
    bitArray.entries(769) = 4194304L
    bitArray.entries(770) = 1688849860263936L
    bitArray.entries(771) = 2048;
    bitArray.entries(773) = 1073741824L
    bitArray.entries(774) = 1080863910568919040L
    bitArray.entries(779) = 14;
    bitArray.entries(783) = 3584;
    bitArray.entries(787) = 393216L
    bitArray.entries(791) = 100663296L
    bitArray.entries(795) = 25769803776L
    bitArray.entries(807) = 432345564227567616L
    bitArray.entries(808) = 4194304L
    bitArray.entries(812) = 12348030982L
    bitArray.entries(816) = 962072674304L
    bitArray.entries(841) = 234881024L
    bitArray.entries(845) = 12884930560L
    bitArray.entries(849) = 3298538029056L
    bitArray.entries(853) = 562950356074496L
    bitArray.entries(861) = -9223372036854775808L
    bitArray.entries(862) = 1;
    bitArray.entries(866) = 896;
    bitArray.entries(870) = 98304L
    bitArray.entries(874) = 251658240L
    bitArray.entries(878) = 68182605824L
    bitArray.entries(882) = 5222680231936L
    bitArray.entries(886) = 211107306274816L
    bitArray.entries(902) = 6918584558803746816L
    bitArray.entries(906) = 270215977642229760L
    bitArray.entries(915) = 1;
    bitArray.entries(919) = 70368744178432L
    bitArray.entries(923) = 27021597764222976L
    bitArray.entries(927) = 6917533425687592960L
    bitArray.entries(928) = 48;
    bitArray.entries(931) = 3377699745693696L
    bitArray.entries(932) = 12400L
    bitArray.entries(935) = 1008806322973442048L
    bitArray.entries(936) = 3170304L
    bitArray.entries(939) = 824633720832L
    bitArray.entries(940) = 12;
    bitArray.entries(944) = 268435456L
    bitArray.entries(962) = 196608L
    bitArray.entries(964) = -4611686018427387904L
    bitArray.entries(965) = 3;
    bitArray.entries(966) = 58720256L
    bitArray.entries(969) = 992;
    bitArray.entries(970) = 15032385536L
    bitArray.entries(973) = 258048L
    bitArray.entries(974) = 3377699720527872L
    bitArray.entries(977) = 16646144L
    bitArray.entries(978) = 864691128455135232L
    bitArray.entries(981) = 4227858432L
    bitArray.entries(985) = 27022113160298496L
    bitArray.entries(989) = 6917555415920148480L
    bitArray.entries(998) = 32768L
    bitArray.entries(1006) = 33554432L
    bitArray.entries(1010) = 12884901889L
    bitArray.entries(1014) = 105553116268288L
    bitArray.entries(1018) = 31525197391790080L
    bitArray.entries(1022) = 4035225266140741632L
    bitArray.entries(1027) = 24;
    bitArray.entries(1031) = 2048L
    bitArray.entries(1051) = 52776558133248L
    bitArray.entries(1055) = 13510798882111488L
    bitArray.entries(1059) = 3462142213541068800L
    bitArray.entries(1063) = 6917529027641081856L
    bitArray.entries(1067) = 216172782113783808L
    bitArray.entries(1088) = 61572651155456L
    val canvas = new VectorCanvas(new Envelope(0, 0, 256, 256), 256, 256, 0, 4)
    canvas.occupiedPixels.inplaceOr(bitArray)
    val rings = canvas.createRingsForOccupiedPixels
    // No assertion is needed. The assertions inside the canvas.createLinearRingsForOccupiedPixels should be enough
  }

  test("Stress test with large canvas") {
    if (shouldRunStressTest()) {
      val width = 1000
      val height = 1000
      val p = 0.6f
      val factory = GeometryReader.DefaultGeometryFactory
      var totalTime = 0L

      for (i <- 0 until 100) {
        val random = new Random(i)
        val canvas = new VectorCanvas(new Envelope(0, width, 0, height),
          width, height, 0, 0)
        for (x <- 0 until width; y <- 0 until height; if random.nextFloat() < p) {
          canvas.addGeometry(factory.createPoint(new CoordinateXY(x, y)), null)
        }
        val t1 = System.nanoTime()
        val rings = canvas.createRingsForOccupiedPixels
        val t2 = System.nanoTime()
        totalTime += (t2 - t1)
      }
      //println(s"Total time is ${totalTime*1E-9} seconds")
    }
  }

}
