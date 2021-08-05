use super::endian::{B32, L64};
use chain_core::property::Serialize as _;
use chain_impl_mockchain::{header::HeaderId, transaction, value::Value};
use sanakirja::{direct_repr, Storable, UnsizedStorable};
use std::{convert::TryInto, mem::size_of};
use zerocopy::{AsBytes, FromBytes};

#[derive(PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct AccountId(pub [u8; chain_impl_mockchain::transaction::INPUT_PTR_SIZE]);
direct_repr!(AccountId);

impl std::fmt::Debug for AccountId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

pub type ProposalIndex = u8;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProposalId {
    pub vote_plan: VotePlanId,
    pub index: ProposalIndex,
}
direct_repr!(ProposalId);

pub type BlockId = StorableHash;

impl From<BlockId> for HeaderId {
    fn from(val: BlockId) -> Self {
        HeaderId::from(val.0)
    }
}

pub type FragmentId = StorableHash;
pub type VotePlanId = StorableHash;

#[derive(Clone, PartialOrd, Ord, PartialEq, Eq, AsBytes, FromBytes)]
#[cfg_attr(test, derive(Hash))]
#[repr(C)]
pub struct StorableHash(pub [u8; 32]);

impl StorableHash {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

direct_repr!(StorableHash);

impl StorableHash {
    pub const MIN: Self = StorableHash([0x00; 32]);
    pub const MAX: Self = StorableHash([0xff; 32]);
}

impl From<chain_impl_mockchain::key::Hash> for StorableHash {
    fn from(id: chain_impl_mockchain::key::Hash) -> Self {
        let bytes: [u8; 32] = id.into();

        Self(bytes)
    }
}

impl From<chain_impl_mockchain::certificate::VotePlanId> for StorableHash {
    fn from(id: chain_impl_mockchain::certificate::VotePlanId) -> Self {
        let bytes: [u8; 32] = id.into();

        Self(bytes)
    }
}

impl From<[u8; 32]> for StorableHash {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl From<StorableHash> for [u8; 32] {
    fn from(wrapper: StorableHash) -> Self {
        wrapper.0
    }
}

impl std::fmt::Debug for StorableHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

pub type SlotId = B32;
pub type EpochNumber = B32;

#[derive(Debug, Clone, AsBytes, FromBytes, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct ChainLength(pub(super) B32);

impl ChainLength {
    pub fn new(n: u32) -> Self {
        Self(B32::new(n))
    }
}

direct_repr!(ChainLength);

impl From<chain_impl_mockchain::block::ChainLength> for ChainLength {
    fn from(c: chain_impl_mockchain::block::ChainLength) -> Self {
        Self(B32::new(u32::from(c)))
    }
}

impl From<u32> for ChainLength {
    fn from(n: u32) -> Self {
        Self(B32::new(n))
    }
}

impl From<&ChainLength> for u32 {
    fn from(n: &ChainLength) -> Self {
        n.0.get()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, AsBytes, FromBytes)]
#[repr(C)]
pub struct BlockDate {
    pub epoch: EpochNumber,
    pub slot_id: SlotId,
}

impl From<chain_impl_mockchain::block::BlockDate> for BlockDate {
    fn from(d: chain_impl_mockchain::block::BlockDate) -> Self {
        Self {
            epoch: B32::new(d.epoch),
            slot_id: B32::new(d.slot_id),
        }
    }
}

pub type PoolId = StorableHash;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, AsBytes)]
#[repr(u8)]
pub enum PayloadType {
    Public = 1,
    Private = 2,
}

impl From<chain_impl_mockchain::vote::PayloadType> for PayloadType {
    fn from(p: chain_impl_mockchain::vote::PayloadType) -> Self {
        match p {
            chain_impl_mockchain::vote::PayloadType::Public => PayloadType::Public,
            chain_impl_mockchain::vote::PayloadType::Private => PayloadType::Private,
        }
    }
}

impl From<PayloadType> for chain_impl_mockchain::vote::PayloadType {
    fn from(p: PayloadType) -> Self {
        match p {
            PayloadType::Public => chain_impl_mockchain::vote::PayloadType::Public,
            PayloadType::Private => chain_impl_mockchain::vote::PayloadType::Private,
        }
    }
}

pub type ExternalProposalId = StorableHash;
pub type Options = u8;

#[derive(Clone, Debug, FromBytes, AsBytes, PartialEq, Eq, Ord, PartialOrd)]
#[repr(C)]
pub struct ExplorerVoteProposal {
    pub proposal_id: ExternalProposalId,
    pub options: Options,
}

impl From<&chain_impl_mockchain::certificate::Proposal> for ExplorerVoteProposal {
    fn from(p: &chain_impl_mockchain::certificate::Proposal) -> Self {
        ExplorerVoteProposal {
            proposal_id: StorableHash::from(<[u8; 32]>::from(p.external_id().clone())),
            options: p.options().choice_range().end,
        }
    }
}

direct_repr!(ExplorerVoteProposal);

pub type Choice = u8;

pub type Stake = L64;

pub const MAX_ADDRESS_SIZE: usize = chain_addr::ADDR_SIZE_GROUP;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, AsBytes, FromBytes)]
#[repr(C)]
pub struct Address(pub [u8; MAX_ADDRESS_SIZE]);

impl Address {
    pub const MIN: Address = Address([0u8; MAX_ADDRESS_SIZE]);
    pub const MAX: Address = Address([255u8; MAX_ADDRESS_SIZE]);
}

direct_repr!(Address);

impl From<chain_addr::Address> for Address {
    fn from(addr: chain_addr::Address) -> Self {
        let mut bytes = [0u8; MAX_ADDRESS_SIZE];
        addr.serialize(&mut bytes[..]).unwrap();
        Self(bytes)
    }
}

impl From<&chain_addr::Address> for Address {
    fn from(addr: &chain_addr::Address) -> Self {
        let mut bytes = [0u8; MAX_ADDRESS_SIZE];
        addr.serialize(&mut bytes[..]).unwrap();
        Self(bytes)
    }
}

impl TryInto<chain_addr::Address> for Address {
    type Error = chain_addr::Error;

    fn try_into(self) -> Result<chain_addr::Address, Self::Error> {
        chain_addr::Address::from_bytes(&self.0[0..33])
            .or_else(|_| chain_addr::Address::from_bytes(&self.0[0..MAX_ADDRESS_SIZE]))
    }
}

impl std::fmt::Debug for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let addr: chain_addr::Address = self.clone().try_into().unwrap();
        // addr.fmt(f)
        //
        f.write_str(&hex::encode(self.0))
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, AsBytes, FromBytes)]
#[repr(C)]
pub struct TransactionInput {
    pub input_ptr: [u8; 32],
    pub value: L64,
    pub utxo_or_account: u8,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
enum InputType {
    Utxo = 0x00,
    Account = 0xff,
}

impl From<&transaction::Input> for TransactionInput {
    fn from(i: &transaction::Input) -> Self {
        TransactionInput {
            input_ptr: i.bytes()[9..].try_into().unwrap(),
            utxo_or_account: match i.get_type() {
                transaction::InputType::Utxo => InputType::Utxo as u8,
                transaction::InputType::Account => InputType::Account as u8,
            },
            value: L64::new(i.value().0),
        }
    }
}

impl From<&TransactionInput> for transaction::Input {
    fn from(input: &TransactionInput) -> Self {
        transaction::Input::new(
            input.utxo_or_account,
            Value(input.value.get()),
            input.input_ptr,
        )
    }
}

direct_repr!(TransactionInput);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, AsBytes, FromBytes)]
#[repr(C)]
pub struct TransactionOutput {
    pub address: Address,
    pub value: L64,
}

impl TransactionOutput {
    pub fn from_original(output: &transaction::Output<chain_addr::Address>) -> Self {
        TransactionOutput {
            address: Address::from(output.address.clone()),
            value: L64::new(output.value.0),
        }
    }
}

impl From<&TransactionOutput> for transaction::Output<chain_addr::Address> {
    fn from(output: &TransactionOutput) -> Self {
        transaction::Output {
            address: output.address.clone().try_into().unwrap(),
            value: Value(output.value.get()),
        }
    }
}

direct_repr!(TransactionOutput);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, AsBytes)]
#[repr(C)]
pub struct TransactionCertificate {
    pub(crate) tag: CertificateTag,
    pub(crate) cert: SerializedCertificate,
}

impl TransactionCertificate {
    const fn alloc() -> [u8; size_of::<SerializedCertificate>()] {
        [0u8; size_of::<SerializedCertificate>()]
    }

    pub fn from_vote_plan_id(id: VotePlanId) -> Self {
        let mut alloc = [0u8; size_of::<SerializedCertificate>()];
        alloc[0..std::mem::size_of_val(&id)].copy_from_slice(id.as_bytes());

        TransactionCertificate {
            tag: CertificateTag::VotePlan,
            cert: SerializedCertificate(alloc),
        }
    }

    pub fn from_public_vote_cast(vote: PublicVoteCast) -> Self {
        let mut alloc = Self::alloc();
        alloc[0..std::mem::size_of_val(&vote)].copy_from_slice(vote.as_bytes());

        TransactionCertificate {
            tag: CertificateTag::PublicVoteCast,
            cert: SerializedCertificate(alloc),
        }
    }

    pub fn into_public_vote_cast(self) -> Option<PublicVoteCast> {
        match self.tag {
            CertificateTag::PublicVoteCast => {
                let bytes: [u8; std::mem::size_of::<PublicVoteCast>()] = self.cert.0
                    [0..std::mem::size_of::<PublicVoteCast>()]
                    .try_into()
                    .unwrap();

                let vote_cast: PublicVoteCast = unsafe { std::mem::transmute(bytes) };

                Some(vote_cast)
            }
            CertificateTag::VotePlan => None,
        }
    }
}

direct_repr!(TransactionCertificate);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, AsBytes)]
#[repr(u8)]
pub enum CertificateTag {
    VotePlan = 0,
    PublicVoteCast = 1,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, AsBytes)]
#[repr(C)]
pub struct SerializedCertificate(
    [u8; max(
        std::mem::size_of::<VotePlanId>(),
        std::mem::size_of::<PublicVoteCast>(),
    )],
);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, AsBytes)]
#[repr(C)]
pub struct PublicVoteCast {
    pub vote_plan_id: VotePlanId,
    pub proposal_index: u8,
    pub choice: Choice,
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, AsBytes)]
#[repr(C)]
pub struct VotePlanMeta {
    pub vote_start: BlockDate,
    pub vote_end: BlockDate,
    pub committee_end: BlockDate,
    pub payload_type: PayloadType,
}

direct_repr!(VotePlanMeta);

const fn max(a: usize, b: usize) -> usize {
    if a > b {
        a
    } else {
        b
    }
}
