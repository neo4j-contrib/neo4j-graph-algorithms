package org.neo4j.graphalgo.core.utils.container;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author mknblch
 */
public class FlipStackTest {

    @Test
    public void testSimpleFlip() throws Exception {

        final FlipStack stack = new FlipStack(3);

        stack.push(1);
        stack.push(2);
        stack.push(3);

        stack.flip();

        assertEquals(3, stack.pop());
        assertEquals(2, stack.pop());
        assertEquals(1, stack.pop());
    }

    @Test
    public void testMultiFlip() throws Exception {

        final FlipStack stack = new FlipStack(1);

        for (int i = 0; i < 10; i++) {
            stack.push(i);
            stack.push(i + 1);
            stack.flip();
            assertEquals(i + 1, stack.pop());
            assertEquals(i, stack.pop());
        }
    }

    @Test
    public void testForEach() throws Exception {

        final FlipStack stack = new FlipStack(3);

        stack.push(1);
        stack.push(2);
        stack.push(3);

        System.out.println(stack);
        stack.flip();
        System.out.println(stack);

        stack.forEach(stack::push);

        System.out.println(stack);

        assertEquals(3, stack.pop());
        assertEquals(2, stack.pop());
        assertEquals(1, stack.pop());

        stack.flip();

        assertEquals(3, stack.pop());
        assertEquals(2, stack.pop());
        assertEquals(1, stack.pop());

    }

    @Test
    public void testFillBoth() throws Exception {

        final FlipStack stack = new FlipStack(3);

        stack.push(1);
        stack.push(2);
        stack.push(3);

        stack.flip();

        stack.push(4);
        stack.push(5);
        stack.push(6);

        assertEquals(3, stack.pop());
        assertEquals(2, stack.pop());
        assertEquals(1, stack.pop());

        stack.flip();

        assertEquals(6, stack.pop());
        assertEquals(5, stack.pop());
        assertEquals(4, stack.pop());

    }
}
