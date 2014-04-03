package voldemort.collections;

/**
 * Wrapper around VListIterator that strips out the version information. Only
 * useful when no further storage operations will be done on these values.
 */
public class VListIteratorValues<E> implements MappedListIterator<Integer, E> {

    private final VListIterator<E> _listIterator;

    public VListIteratorValues(VListIterator<E> vListIterator) {
        _listIterator = vListIterator;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#add(java.lang.Object)
     */
    @Override
    public void add(E arg0) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return _listIterator.hasNext();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#hasPrevious()
     */
    @Override
    public boolean hasPrevious() {
        return _listIterator.hasPrevious();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#next()
     */
    @Override
    public E next() {
        return _listIterator.next().getValue();
    }

    @Override
    public Integer nextId() {
        return _listIterator.nextId();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#previous()
     */
    @Override
    public E previous() {
        return _listIterator.previous().getValue();
    }

    @Override
    public Integer previousId() {
        return _listIterator.previousId();
    }

    @Override
    public Integer lastId() {
        return _listIterator.lastId();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#remove()
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#set(java.lang.Object)
     */
    @Override
    public void set(E arg0) {
        throw new UnsupportedOperationException();
    }

}
