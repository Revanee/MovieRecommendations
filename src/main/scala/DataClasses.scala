object DataClasses {
  case class Rating(user: Int, movie: Int, score: Double)
  case class Similarity(user: Int, otherUser: Int, score: Double)
}
