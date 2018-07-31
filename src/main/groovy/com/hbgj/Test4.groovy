package com.hbgj

class Rectangle {
    void draw() {
        println "Rectangel draw"
    }
}
class Circle {
    void draw() {
        println "Circle draw"
    }
    def getRadius() {
        return 10
    }
}
class Oval {
    void draw() {
        println "Oval draw"
    }
}
void draw(shape) {
    shape.draw()

    if (shape.metaClass.respondsTo(shape, "getRadius")) {
        println "radius = ${shape.getRadius()}"
    }
}
draw(new Rectangle())
draw(new Circle())
draw(new Oval())
