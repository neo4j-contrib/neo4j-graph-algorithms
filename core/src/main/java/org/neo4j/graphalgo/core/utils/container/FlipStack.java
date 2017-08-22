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

    public FlipStack(IntContainer nodes) {
        this(nodes.size());
        addAll(nodes);
    }

    public void reset() {
        flip = 0;
        pushStack().clear();
        popStack().clear();
    }

    /**
     * flip stacks. current popStack becomes pushStack
     * and vice versa
     */
    public void flip() {
        flip++;
    }

    /**
     * add all elements to the current pushStack
     *
     * @param container
     */
    public void addAll(IntContainer container) {
        pushStack().addAll(container);
    }

    /**
     * push value onto the pushStack
     *
     * @param value
     */
    public void push(int value) {
        pushStack().push(value);
    }

    /**
     * pop head of the popStack
     *
     * @return head of the popStack
     */
    public int pop() {
        return popStack().pop();
    }

    /**
     * check if the popStack is empty
     *
     * @return
     */
    public boolean isEmpty() {
        return popStack().isEmpty();
    }

    /**
     * for each element of the popStack
     *
     * @param consumer
     */
    public void forEach(IntConsumer consumer) {
        popStack().forEach((IntProcedure) consumer::accept);
    }

    /**
     * retrieve the pushStack
     *
     * @return the pushStack
     */
    public IntStack pushStack() {
        return stacks[flip % 2];
    }

    /**
     * retrieve the popStack
     *
     * @return popStack
     */
    public IntStack popStack() {
        return stacks[(flip + 1) % 2];
    }

/*
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
*/
}
