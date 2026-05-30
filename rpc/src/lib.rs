use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum Message {
    Text(String),
    Rock,
    Paper,
    Scissors,
}

// messages sent from a websocket stream might not be aligned to what rkyv wants
pub fn decode_message(bytes: &[u8]) -> Result<Message, rkyv::rancor::Error> {
    let mut aligned: rkyv::util::AlignedVec = rkyv::util::AlignedVec::new();
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<Message, rkyv::rancor::Error>(aligned.as_ref())
}
