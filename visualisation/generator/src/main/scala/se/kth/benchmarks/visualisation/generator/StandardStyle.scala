package se.kth.benchmarks.visualisation.generator

import scalatags.Text.all._
import scalatags.stylesheet._

object StandardStyle extends StyleSheet {
  override def customSheetName = Some("mpp");

  initStyleSheet()

  val headline = cls(
    color := "#FF0000"
  );
}
