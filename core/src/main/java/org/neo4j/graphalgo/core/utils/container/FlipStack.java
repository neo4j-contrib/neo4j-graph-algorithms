package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntStack;
import com.carrotsearch.hppc.procedures.IntProcedure;

import java.util.function.IntConsumer;

/**
 * flippable Stack impl. where pop() and push() operations work on distinct stacks
 *
 * @author mknblch
 */
public class FlipStack {

    private final IntStack[] stacks;
    private int flip = 0;

    public FlipStack(int nodeCount) {

        stacks = new IntStack[]{
                new IntStack(nodeCount),
                new IntStack(nodeCount)
        };
    }

    public void reset() {
        flip = 0;
        pushStack().clear();
        popStack().clear();
    }

    public void flip() {
        flip++;
    }

    public void addAll(IntContainer container) {
        pushStack().addAll(container);
    }

    public void push(int value) {
        pushStack().push(value);
    }

    public int pop() {
        return popStack().pop();
    }

    public boolean isEmpty() {
        return popStack().isEmpty();
    }

    public void clear() {
        popStack().clear();
    }

    public void forEach(IntConsumer consumer) {
        popStack().forEach((IntProcedure) consumer::accept);
    }

    private IntStack pushStack() {
        return stacks[flip % 2];
    }

    private IntStack popStack() {
        return stacks[(flip + 1) % 2];
    }

    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("Stack[0]: ");
        for (int i = 0; i < pushStack().size(); i++) {
            stringBuilder.append(String.format("%2d ", pushStack().get(i)));
        }
        stringBuilder.append("\nStack[1]: ");
        for (int i = 0; i < popStack().size(); i++) {
            stringBuilder.append(String.format("%2d ", popStack().get(i)));
        }
        stringBuilder.append("\n");

        return stringBuilder.toString();
    }
}