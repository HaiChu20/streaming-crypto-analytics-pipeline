use crate::Book;

/// Top 25 bids descending and asks ascending, interleaved; native decimal text.
pub(crate) fn checksum(book: &Book) -> i32 {
    let bids = book.bids.values().rev().take(25).collect::<Vec<_>>();
    let asks = book.asks.values().take(25).collect::<Vec<_>>();
    let mut fields = Vec::new();
    for i in 0..bids.len().max(asks.len()) {
        for levels in [&bids, &asks] {
            if let Some(level) = levels.get(i) {
                fields.push(level.price_text.as_str());
                fields.push(level.size_text.as_str());
            }
        }
    }
    crc32fast::hash(fields.join(":").as_bytes()) as i32
}
