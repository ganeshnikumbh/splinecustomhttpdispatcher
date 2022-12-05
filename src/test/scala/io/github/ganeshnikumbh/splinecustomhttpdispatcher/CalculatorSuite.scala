package io.github.ganeshnikumbh.splinecustomhttpdispatcher

import org.scalatest.FunSpec

class CalculatorSpec extends FunSpec {
  val calculator = new Calculator

  // can nest as many levels deep as you like
  describe("multiplication") {
    it("should give back 0 if multiplying by 0") {
      assert(calculator.multiply(572389, 0) == 0)
      assert(calculator.multiply(-572389, 0) == 0)
      assert(calculator.multiply(0, 0) == 0)
    }
  }
}