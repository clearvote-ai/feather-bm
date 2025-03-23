class Node {
    data: number;
    next: Node | null;

    constructor(data: number) {
        this.data = data;
        this.next = null;
    }
}
  
class SortedLinkedList {
    head: Node | null;

    constructor() {
        this.head = null;
    }

    insert(data: number) {
        const newNode = new Node(data);

        if (!this.head || data < this.head.data) {
            newNode.next = this.head;
            this.head = newNode;
        } else {
            let current = this.head;
            while (current.next && current.next.data < data) {
                current = current.next;
            }
            newNode.next = current.next;
            current.next = newNode;
        }
    }

    rangeQuery(min: number, max: number): number[] {
        const result: number[] = [];
        let current = this.head;

        while (current) {
            if (current.data >= min && current.data <= max) {
                result.push(current.data);
            }
            current = current.next;
        }

        return result;
    }
}