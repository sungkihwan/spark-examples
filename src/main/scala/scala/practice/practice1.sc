  // val -> 이뮤터블 -> 불변 -> const
  val hello: String = "Hola!"

  // var -> 뮤터블 -> var
  var hello2: String = hello
  hello2 = hello + " hello2"
  println(hello2)

  // Data Types
  val number: Int = 1
  val truth: Boolean = true
  val letter: Char = 'a'
  val pi: Double = 3.141592
  val pi2: Float = 3.14159265f
  val longNum: Long = 1231251111
  val byte: Byte = 127

  println(number + letter + letter + pi + pi2)

  println(f"Pi is about $pi2%.3f")