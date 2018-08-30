object DataClasses {
  case class Rating(user: Int, movie: Int, score: Double)
  case class Similarity(user: Int, otherUser: Int, score: Double)
  case class MatrixEntry(x: Double, y: Double, xx: Double, yy: Double, xy: Double, n: Int = 1)
}
